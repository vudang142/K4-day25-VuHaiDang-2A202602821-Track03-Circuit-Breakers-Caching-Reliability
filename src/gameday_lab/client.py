"""Part C — ResilientClient: compose classification, retry, breaker and fallback.

The simulator calls ``start`` for every logical LLM call, then ``on_success``
or ``on_error`` for every attempt. You answer with ``Send`` (try a provider,
maybe after a delay) or ``GiveUp`` (user gets a static degraded answer).

Tests: tests/test_client.py, then `make compare` (6 incident tapes).

Per-call scratch space lives in ``call.state`` (a dict). Suggested keys:
    "ladder": list[str]  providers this call may use, in order
    "idx":    int        position of the provider we are currently on
    "tries":  int        attempts already made on that provider

Decision table for ``on_error`` (implement it!):

| ErrorKind        | breaker              | then                                                    |
|------------------|----------------------|---------------------------------------------------------|
| CLIENT_ERROR     | release (not health) | GiveUp("client_error") — never retry, never fail over   |
| QUOTA_EXHAUSTED  | release              | cooldown provider for quota_cooldown_s; fail over now   |
| RATE_LIMITED     | release              | cooldown provider until now + retry_after; fail over now|
| OVERLOADED /     | on_failure           | retry SAME provider after backoff.delay(tries) if       |
| SERVER_ERROR /   |                      |   tries < max_attempts_per_provider AND budget allows   |
| TIMEOUT          |                      |   AND it fits the deadline; otherwise fail over         |

Cooldowns are per provider and shared by all calls (other calls must avoid a
rate-limited provider too). When every provider in the ladder is unusable,
you may wait for a *short* (<= 5 s) rate-limit cooldown if it fits the deadline.
"""

from __future__ import annotations

import random
from typing import Any

from gameday_lab.breaker import BreakerState, Permit, SlidingWindowBreaker  # noqa: F401
from gameday_lab.errors import ErrorKind, classify, counts_toward_breaker  # noqa: F401
from gameday_lab.retry import FullJitterBackoff, RetryBudget
from gameday_lab.sim.contracts import (
    Action,
    Attempt,
    BaseClient,
    CallContext,
    GiveUp,
    ProviderError,
    ProviderInfo,
    Send,
)

RATE_LIMIT_WAIT_MAX_S = 5.0  # only wait for short (rate-limit) cooldowns, never for quota


class ResilientClient(BaseClient):
    name = "resilient"

    def __init__(
        self,
        providers: dict[str, ProviderInfo],
        fallback_order: list[str] | tuple[str, ...],
        settings: dict[str, Any],
        seed: int = 0,
    ) -> None:
        """PROVIDED: reads the `client:` block of configs/gameday.yaml."""
        self.providers = providers
        self.order = list(fallback_order)
        self.timeout_s = float(settings.get("attempt_timeout_s", 8.0))
        self.max_attempts_per_provider = int(settings.get("max_attempts_per_provider", 2))
        self.min_time_left_s = float(settings.get("min_time_left_s", 1.0))
        self.quota_cooldown_s = float(settings.get("quota_cooldown_s", 300.0))
        self.backoff = FullJitterBackoff(
            float(settings.get("backoff_base_s", 0.25)),
            float(settings.get("backoff_cap_s", 4.0)),
            random.Random(seed),
        )
        self.budget = RetryBudget(
            float(settings.get("retry_budget_ratio", 0.1)),
            float(settings.get("retry_budget_min_per_s", 0.5)),
            float(settings.get("retry_budget_max_tokens", 20.0)),
        )
        breaker_cfg = dict(settings.get("breaker", {}))
        self.breakers = {name: SlidingWindowBreaker(name, **breaker_cfg) for name in providers}
        self.cooldown_until: dict[str, float] = {name: 0.0 for name in providers}

    # ------------------------------------------------------------------ provided helper
    def _send(self, call: CallContext, provider: str, now: float, delay: float) -> Action | None:
        """PROVIDED. Build a Send for ``provider`` (after ``delay``), or None if its breaker refuses.

        Takes a breaker permit (carried in ``tag``), and caps the attempt timeout
        so it never runs past the call's deadline. Returns GiveUp("deadline") if
        there is not enough time left for a meaningful attempt.
        """
        permit = self.breakers[provider].allow(now)
        if permit is None:
            return None
        timeout = min(self.timeout_s, call.deadline - (now + delay))
        if timeout < self.min_time_left_s:
            self.breakers[provider].release(permit, now)
            return GiveUp("deadline")
        return Send(provider, timeout_s=timeout, delay_s=delay, tag=permit)

    # ------------------------------------------------------------------ TODO
    def ladder(self, task: str) -> list[str]:
        """``self.order`` filtered by capability —
        a "tool" call may only use providers with ``supports_tools``."""
        if task == "tool":
            return [p for p in self.order if self.providers[p].supports_tools]
        return list(self.order)

    def _route(self, call: CallContext, now: float, delay: float = 0.0) -> Action:
        """Pick the provider for the next attempt.

        Walk the ladder from call.state["idx"]:
          * skip a provider still in cooldown at send time (now + delay)
          * otherwise try self._send(...); a non-None result is your answer
          * when skipping, move idx forward, reset tries, and set delay = 0
            (a failover goes out immediately)
        If the ladder is exhausted: optionally wait for the earliest *short*
        rate-limit cooldown (<= RATE_LIMIT_WAIT_MAX_S, breaker not OPEN, fits the
        deadline), else GiveUp("no_provider_available").
        """
        ladder = call.state["ladder"]
        idx = call.state["idx"]
        # skip providers in cooldown at send time
        while idx < len(ladder):
            provider = ladder[idx]
            send_at = now + delay
            if self.cooldown_until[provider] <= send_at:
                # try to get a permit (breaker may transition OPEN -> HALF_OPEN)
                action = self._send(call, provider, now, delay)
                if action is not None:
                    if isinstance(action, Send):
                        call.state["current_provider"] = provider
                    return action
            # breaker refused or cooldown in effect at send time: skip
            idx += 1
            call.state["idx"] = idx
            call.state["tries"] = 0
            delay = 0.0  # failover: immediate

        # ladder exhausted: optionally wait for a short rate-limit cooldown
        candidates: list[tuple[float, str]] = []
        for provider in ladder:
            cd = self.cooldown_until[provider]
            wait = cd - now
            # only rate-limit style cooldowns, not quota (handled above by failing over)
            # i.e. wait must be short (<= RATE_LIMIT_WAIT_MAX_S), breaker not OPEN
            if 0 < wait <= RATE_LIMIT_WAIT_MAX_S and self.breakers[provider].state is not BreakerState.OPEN:
                if now + wait + self.timeout_s <= call.deadline:
                    candidates.append((wait, provider))
        if candidates:
            candidates.sort()
            wait, provider = candidates[0]
            action = self._send(call, provider, now, wait)
            if action is not None and isinstance(action, Send):
                call.state["current_provider"] = provider
                return action
        return GiveUp("no_provider_available")

    def start(self, call: CallContext, now: float) -> Action:
        """Record the request in the retry budget, initialise call.state, route."""
        self.budget.record_request(now)
        call.state["ladder"] = self.ladder(call.task)
        call.state["idx"] = 0
        call.state["tries"] = 0
        call.state["current_provider"] = None
        if not call.state["ladder"]:
            return GiveUp("no_capable_provider")
        return self._route(call, now, delay=0.0)

    def on_success(self, attempt: Attempt, now: float, latency_s: float) -> None:
        """Report success + latency to the provider's breaker (attempt.tag is the Permit)."""
        permit = attempt.tag
        if permit is not None and isinstance(permit, Permit):
            self.breakers[attempt.provider].on_success(permit, now, latency_s)

    def on_error(self, attempt: Attempt, err: ProviderError, now: float) -> Action:
        """Implement the decision table in the module docstring."""
        provider = attempt.provider
        permit = attempt.tag
        kind = classify(err)

        if kind is ErrorKind.CLIENT_ERROR:
            # release any breaker slot; never retry, never fail over
            if isinstance(permit, Permit):
                self.breakers[provider].release(permit, now)
            return GiveUp("client_error")

        if kind is ErrorKind.QUOTA_EXHAUSTED:
            if isinstance(permit, Permit):
                self.breakers[provider].release(permit, now)
            # long cooldown; fail over immediately
            self.cooldown_until[provider] = now + self.quota_cooldown_s
            call = attempt.call
            call.state["idx"] += 1
            call.state["tries"] = 0
            if call.state["idx"] >= len(call.state["ladder"]):
                return GiveUp("no_provider_available")
            return self._route(call, now, delay=0.0)

        if kind is ErrorKind.RATE_LIMITED:
            if isinstance(permit, Permit):
                self.breakers[provider].release(permit, now)
            ra = err.retry_after if err.retry_after is not None else 1.0
            self.cooldown_until[provider] = now + ra
            call = attempt.call
            call.state["idx"] += 1
            call.state["tries"] = 0
            if call.state["idx"] >= len(call.state["ladder"]):
                return GiveUp("no_provider_available")
            return self._route(call, now, delay=0.0)

        # OVERLOADED / SERVER_ERROR / TIMEOUT — count toward breaker
        if isinstance(permit, Permit):
            self.breakers[provider].on_failure(permit, now)

        call = attempt.call
        call.state["tries"] += 1
        tries = call.state["tries"]
        # can we retry the same provider?
        if tries < self.max_attempts_per_provider and self.budget.try_acquire_retry(now):
            # use retry_after when present (slow_call_s etc. don't carry it but overload may not)
            ra = err.retry_after
            delay = self.backoff.delay(tries, retry_after=ra)
            # cap by deadline
            if now + delay + self.timeout_s > call.deadline:
                # not enough time for a same-provider retry; fail over
                pass
            else:
                action = self._send(call, provider, now, delay)
                if action is not None and isinstance(action, Send):
                    return action
                # breaker refused or deadline too tight: fall through to failover
        # fail over to next provider
        call.state["idx"] += 1
        call.state["tries"] = 0
        if call.state["idx"] >= len(call.state["ladder"]):
            return GiveUp("no_provider_available")
        return self._route(call, now, delay=0.0)

    def on_cancel(self, call: CallContext, tag: Any, now: float) -> None:
        """A delayed attempt was dropped (deadline) — release its permit."""
        if isinstance(tag, Permit):
            self.breakers[tag.breaker].release(tag, now)
