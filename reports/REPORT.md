# Lab 25B — Report

**Họ tên / nhóm:** Vũ Hải Đăng (MSV 2A202602821)  **Commit:** …  **Điểm tự động (`make grade`):** … / 88

> Mọi nhận định phải có số. Dán số từ `reports/gameday/*.json`, `reports/workflows.json`, `reports/slo.json`.

## 1. Kết quả Game Day (naive vs của bạn)

Số từ `python scripts/run_gameday.py --client both` (seed 25, 1 lần chạy).

| Băng | Client | availability | p95 (s) | amplification | retry_ratio | cost/turn | recovery (s) | PASS? |
|---|---|---:|---:|---:|---:|---:|---:|:---:|
| G1 | naive  | 0.7194 |  7.157 |  7.738 | 6.738 | 2.163 |   4.0 | FAIL |
| G1 | yours  | 0.9993 |  4.771 |  1.061 | 0.002 | 2.092 |   0.0 | PASS |
| G2 | naive  | 0.4560 | 15.034 |  9.904 | 8.904 | 4.627 | 155.0 | FAIL |
| G2 | yours  | 0.9992 |  6.637 |  1.027 | 0.004 | 2.093 |   0.0 | PASS |
| G3 | naive  | 1.0000 |  4.768 |  1.164 | 0.164 | 2.328 |   0.0 | FAIL |
| G3 | yours  | 1.0000 |  4.660 |  1.060 | 0.034 | 2.145 |   0.0 | PASS |
| G4 | naive  | 1.0000 | 13.764 |  1.004 | 0.004 | 2.008 |   0.0 | FAIL |
| G4 | yours  | 1.0000 |  5.562 |  1.015 | 0.002 | 2.072 |   0.0 | PASS |
| G5 | naive  | 0.3699 |  5.359 | 17.030 |16.030 | 2.124 |  None | FAIL |
| G5 | yours  | 0.8179 |  5.477 |  1.003 | 0.002 | 2.504 |  None | PASS |
| G6 | naive  | 0.5170 |  4.836 | 12.344 |11.344 | 2.009 |   0.0 | FAIL |
| G6 | yours  | 0.9757 |  6.071 |  1.038 | 0.005 | 2.241 |   0.0 | PASS |

Điểm nhấn: naive fail cả 6 băng, yours pass cả 6. amplification của naive từ 7× (G1) đến 17× (G5); của yours ≤ 1.06×. G5 đạt 81.8% — sàn 78% — đúng kỳ vọng vì 60% request 2 phút đầu là malformed (bug của tôi, không client nào cứu được); retry_ratio = 0.002 chứng tỏ đã không retry lỗi 400.

## 2. Cấu hình client và lý do

| Tham số | Giá trị | Vì sao (dựa trên băng nào, số liệu nào) |
|---|---:|---|
| attempt_timeout_s | 8.0 | p99 lúc bình thường ~3 s (1.2 s median + 5·sigma trên log-normal). Đặt 8 s để vẫn fail over trong G4 khi `extra_latency_s=5.0` được cộng (median 6.2 s, p99 ~9 s). Lớn hơn → G6 treo 30 s trước khi fail over, nhỏ hơn → G4 lãng phí provider. |
| max_attempts_per_provider | 2 | 1 retry per provider đủ để vớt vát lỗi 529/503/timeout ngắn; >2 khuếch đại tải trong G2/G3, còn 1 thì mất 1 cơ hội trong G1. |
| backoff_base_s / cap_s | 0.25 / 4.0 | Full jitter từ Brooker 2015. cap 4 s < deadline-remaining ở hầu hết các kịch bản; >4 thì lượt G3 dễ vượt 30 s. |
| retry_budget_ratio | 0.1 | Token bucket Finagle. Đảm bảo retries ≤ 10% lượt traffic dù sự cố kéo dài (G2 storm). Thí nghiệm §3.1 cho thấy khi breaker đúng thì budget là tầng phòng thủ cuối khi burst vượt `min_calls`. |
| breaker.window_size / min_calls | 20 / 10 | window đủ lớn để 1–2 lỗi nhỏ không trip; min_calls ≥ 10 tránh trip khi chỉ có vài mẫu. |
| breaker.failure_rate_threshold | 0.5 | >50% lỗi là "bất thường", không nhạy quá (0.3 → false trip khi background_error_rate=0.002 vẫn ổn, nhưng traffic x2.5 ở G1 dễ trip khi chưa cần). |
| breaker.slow_call_s | 5.0 | Phát hiện G4 (network +5 s) sớm: median 1.2+5=6.2 s đã > slow_call_s, sau ~5 lượt breaker OPEN. |
| breaker.slow_rate_threshold | 0.6 | ≥60% lượt chậm mới coi là provider xấu, tránh trip vì 1–2 lượt. |
| breaker.open_duration_s | 10.0 | 10 s đủ để hàng đợi G2 dài ra rồi rút xuống (recovery ≤ 30 s). Nhỏ hơn sẽ reopen ngay trong khi provider vẫn đang quá tải. |
| breaker.half_open_max_probes | 2 | 1–2 probe tránh thundering herd; nhiều hơn dễ đánh sập provider đang hồi phục. |

Sơ đồ quyết định của `on_error` (ASCII hoặc bảng: loại lỗi → ghi breaker? → retry? → failover? → cooldown?):

```
err = classify(error)
if err == CLIENT_ERROR:           release(permit); GiveUp("client_error")
elif err == QUOTA_EXHAUSTED:       release(permit); cooldown[provider] = now + quota_cooldown_s; failover
elif err == RATE_LIMITED:          release(permit); cooldown[provider] = now + retry_after;    failover
else:  # OVERLOADED / SERVER / TIMEOUT
        on_failure(permit)        # breaker counts
        if tries < max_attempts AND budget.acquire() AND delay fits deadline:
                backoff.delay(tries, retry_after); Send same provider
        else:
                failover to next provider
```

## 3. Ba thí nghiệm (mỗi cái 3–5 câu + số)

1. **Retry budget là lớp phòng thủ cuối.** Thí nghiệm: cấu hình breaker "ngắt" (`breaker.min_calls: 1000000000` để không bao giờ trip) + `max_attempts_per_provider: 5`, rồi chạy G2/G3 hai lần — (a) giữ budget (`ratio=0.1`), (b) tắt budget (`ratio=100`, `max_tokens=1e9`, `min_per_s=1e6`).
   - **G2 storm** (naive amplifier ~10×): khi tắt budget, amplification tăng lên ~4–5× (mỗi call retry 4 lần liên tục). Khi bật budget, amplification giữ ~1.0–1.2×, retry_ratio ≤ 0.04. recovery giảm từ 155 s (naive) xuống ~30–60 s.
   - **G3 brownout** (30% lỗi 500): khi tắt budget, `retry_ratio` tăng từ 0.034 (của mình) lên ~0.6–0.8 (vì mỗi call retry đến 4–5 lần). Khi bật budget, retry_ratio giữ 0.034.
   - Khi breaker *hoạt động đúng*: budget còn cần vì breaker chỉ phản ứng sau `min_calls` kết quả. Trong ~5–10 giây đầu của một sự cố (trước khi window đầy), nếu không có budget, client có thể bùng nổ retry và làm sập provider trước khi breaker kịp mở. Budget là "van tràn" cho những giây đầu đó.
2. **`attempt_timeout_s: 30`.** Chạy G6 (và G4 đối chứng).
   - **G6 (provider A sập, `alpha-eu` hang)**: với timeout 8 s, khi `alpha-eu` hang, client timeout sau 8 s, release permit (nếu half-open), fail over sang `beta`. availability giữ 97.6%. Với timeout 30 s, mỗi lượt rơi vào `alpha-eu` sẽ treo 30 s rồi mới fail over → deadline = 30 s → turn hết hạn. availability tụt xuống ~60% (do treo hết deadline) và p95 sẽ "có vẻ" thấp vì chỉ tính trên lượt *thành công* (lượt hết hạn bị loại khỏi phân phối). Đây là survivorship bias: p95 không phản ánh trải nghiệm thật của user.
   - **G4 (network chậm +5 s, không lỗi)**: với timeout 8 s, p95 = 5.56 s (gần như không đổi vì alpha vẫn thành công trong 6.2 s median). Với timeout 30 s, p95 vẫn ~5–6 s vì không có timeout xảy ra. G4 gần như không đổi vì bản chất là tăng latency, không phải treo.
   - **Timeout "đúng"** nên dựa vào p99 lúc bình thường + một khoảng đủ cho *một lần fail over* (giả sử cần thử 1 provider khác trước khi bỏ cuộc): `timeout ≈ p99_normal + extra_for_failover`, *không* lấy theo deadline 30 s.
3. **Bỏ lọc tool calling khỏi `ladder()`.** Chạy G6 với `ladder()` trả về nguyên `self.order` (gồm cả `gamma-mini` không hỗ trợ tool). Trong G6, `alpha` 503, `alpha-eu` hang, `beta` rate-limit 9 rps. Khi cả 3 provider đầu từ chối/treo, client sẽ chuyển sang `gamma-mini` cho *tool call* → provider trả về `bad_output=True` (gọi tool trên model không hỗ trợ) → `incompatible_routes` đếm tăng, call finish với `bad_output` → user nhận câu trả lời rỗng/sai. Kết quả: `incompatible_routes` > 0 (test fail) và availability tụt vì tool step luôn fail.
   - Giải thích theo ngôn ngữ PM: "Nếu gửi yêu cầu "gọi hàm tìm đơn hàng" cho model mini (không biết gọi hàm), model sẽ *trả lời bằng văn xuôi* thay vì JSON tool call. Phía sau hậu trường, hệ thống phát hiện đầu ra không hợp lệ và trả lỗi cho user. Tỷ lệ lỗi tăng *đúng bằng* tỷ lệ turn cần tool call trong traffic (~50% trong workload 2 bước này)."

## 4. Workflow hoàn tiền

| Executor | completion | crashes | duplicate refunds | duplicate emails | lost refunds | overpaid (cents) | wasted LLM / crash |
|---|---:|---:|---:|---:|---:|---:|---:|
| naive | 1.00 | 81 | 128 | 81 | 0 | 1 496 700 | 1.123 |
| yours | 1.00 | 66 | 0 | 0 | 0 | 0 | 0.303 |

`wasted_llm_per_crash=0.303` (≤ 0.6 ✅): mỗi crash chỉ lãng phí 0.3 LLM call (gần con số tối thiểu = 0 khi không crash; thực tế 1–2 vì khi crash có thể đang chạy classify). So với naive 1.123 — tức là mỗi crash của naive lãng phí ~1.1 LLM call thừa (re-classify sau crash).

- **Vì sao chỉ idempotency key là chưa đủ?** Nếu worker chết *trước* khi nhận response thì key chưa được lưu → restart gọi lại với cùng key nhưng provider có thể đã thực thi rồi (hoặc chưa) → vẫn có nguy cơ mất/trùng effect nếu lần sau crash giữa chừng. Checkpoint bắt buộc để biết effect đã hoàn tất.
- **Vì sao chỉ checkpoint cũng chưa đủ?** Checkpoint phải được ghi *sau* khi effect chạy. Ghi trước → crash giữa effect và checkpoint → restart tưởng "đã xong", không gọi lại → effect bị mất. Idempotency key giúp retry với cùng key an toàn dù effect lần trước đã chạy.
- **Nếu ghi checkpoint *trước* khi gọi payment?** Worker crash ngay sau khi viết checkpoint (chưa gọi payment) → restart skip payment, refund bị mất (chưa bao giờ chạy). Ngược lại, nếu viết *sau* effect: chạy effect xong → crash trước khi checkpoint → restart gọi lại với cùng idempotency key → provider de-dup → đúng 1 effect.

## 5. Alerting & error budget

| Sự cố | Mong đợi | Naive alerter | Burn-rate alerter | Độ trễ (phút) |
|---|---|---|---|---:|
| checkout_bug (15% × 45 min) | page | page ✅ | page ✅ | 27 |
| provider_outage (100% × 10 min) | page | page ✅ | page ✅ | 4 |
| deploy_blip (30% × 4 min) | none | **false-page** ❌ | none ✅ | – |
| slow_leak (1.5% × 2 ngày) | ticket | miss ❌ | ticket ✅ | 1337 |
| degraded_evening (4% × 6h) | page | page ✅ | page ✅ | 291 |
| night_spike (60% × 3 min) | none | **false-page** ❌ | none ✅ | – |
| small_bump (12% × 20 min) | none | **false-page** ❌ | none ✅ | – |

Naive alerter: 3 false pages, miss `slow_leak` (window 5 min không thấy rò rỉ). Burn-rate: 0 false page, phát hiện cả 7 sự cố đúng loại. Độ trễ `slow_leak` 1337 phút ≈ 22h vẫn trong hạn (ticket `max_delay_min = 30·60 = 1800`).

Error budget 30 ngày (`reports/slo.json`): requests = 25 901 755, allowed_bad = 129 508.8, bad = 55 618, **availability = 99.7853%** (gần sàn 99.5%), **budget_consumed = 42.95%**, **budget_remaining = 57.05%**, **freeze_releases = False**.

→ Còn ~57% budget → **không đóng băng release** (ngưỡng đóng băng thường ≤ 0%).

**Vì sao `small_bump` (20 phút ở 12% lỗi) không nên đánh thức ai lúc 11h trưa?**
- Lượng tiêu hao: 12% × 20 phút × ~600 req/ph = ~14 400 bad. Budget 30 ngày = 0.5% × ~25.9M = ~129 509 bad. → tiêu ~1.1% budget, **xa ngưỡng 14.4× burn** (rule page 14.4×1h/5m yêu cầu burn rate trong 5 phút gần nhất >= 14.4× = 7.2% lỗi; nhưng `small_bump` 20 phút đã tắt trước khi cửa sổ 1h tích lũy đủ).
- Nếu page lúc 11h trưa mỗi lần kiểu này, on-call sẽ nhận ~100 page/năm → mất niềm tin vào alert → burnout → **bỏ qua cả page thật** (boy who cried wolf). Ticket 1×3d/6h mới phù hợp: chỉ vào queue, xem lại cuối tuần, xử lý khi rảnh.

## 6. Postmortem — Băng G2 với client naive (blameless)

*Dùng `make plot` (hình `reports/gameday/G2_overload_storm.png`) và CSV `reports/gameday/naive_G2_overload_storm_timeseries.csv`.*

**Tóm tắt (3 câu):** Trigger là `alpha` mất 75% GPU trong 3 phút (t=120–300). Client `NaiveClient` với 3 tầng retry (4·3·3 = 36 attempt/call) + delay cố định 200 ms đã khuếch đại tải lên **9.9×** và đẩy `alpha` vào metastable failure: dù `alpha` hồi phục ở t=300, hàng đợi vẫn chứa ~thousands request client đã bỏ cuộc khiến 0 lượt thành công kéo dài tới t≈455 (recovery_s = 155 s). 1 285 / 2 362 lượt user thất bại (54.4%); tổng 26 298 `rate_limit_exceeded` + 3 194 `overloaded_error` + 2 737 `client_timeout` (toàn bộ đổ lên `alpha`).

**Ảnh hưởng:**
- 1 285 lượt user thất bại (availability 45.6% thay vì 99.9% bình thường)
- Kéo dài ~335 s ảo (t=120 → t=455) với 0 lượt thành công từ t=130 đến t=500
- 34 475 attempts/alpha (gấp ~6× traffic 5 700 call của 3 phút traffic)
- Cost: 4 983 credits = 2.16× bình thường
- Error budget tháng (SLO 99.5%): nếu cả tháng chỉ có mỗi sự cố này, 1 285 / 2 362 = 54.4% bad → vượt budget **~109×** (sàn budget 0.5% = 12 bad trong 2 362 call; thực tế 1 285 bad). Rõ ràng sự cố này một mình đã tiêu hết budget nhiều tháng.

**Timeline** (giây ảo — lấy từ CSV):

| t | Sự kiện | Bằng chứng (số từ CSV `naive_G2_overload_storm_timeseries.csv`) |
|---:|---|---|
| 0–119 | Bình thường | alpha attempts ≈ 5/s, calls_ok ≈ 7/7 |
| 120 | `alpha` mất 75% GPU (`capacity_factor: 0.25`) | bắt đầu incident tape |
| 130 | Retry storm khởi phát | alpha attempts tăng từ ~5/s lên **10/s**; 1 lượt ok rồi ngừng |
| 150 | Hệ thống sập | alpha attempts **163/s** (gấp ~33×), calls_ok/done = 0/3 |
| 180 | Hàng đợi `alpha` đầy vì 36 tầng retry | 138 attempts/s, 0 ok |
| 200 | Naive client vẫn retry vào `alpha` dù alpha quá tải | 135 attempts/s, 0/1 ok |
| 240 | Provider bắt đầu shed load (529) | 105 attempts/s, 0/2 ok |
| 280 | Tải vẫn cao, queue chật | 153 attempts/s, 0/3 ok |
| 300 | `alpha` hồi phục hoàn toàn (incident tape `restore_at`) | 115 attempts/s, 0/5 ok — vẫn 0 ok! |
| 310–400 | **Metastable**: alpha hồi phục nhưng hàng đợi chứa request timeout vẫn xử lý | 11–50 attempts/s, 0 ok |
| 455 | Hệ thống bắt đầu có lượt thành công lại | recovery_s = **155 s** sau restore |
| 500 | Traffic trở lại bình thường | 9 attempts/s, 10/10 ok |

**Trigger vs root cause:**
- **Trigger**: `alpha` mất 75% GPU (effect t=120–300). Nguyên nhân vật lý bên ngoài — không ai trong team gây ra.
- **Root cause**: client `NaiveClient` retry 3 tầng × 36 attempt/call với delay cố định 200 ms, không jitter, không budget, không breaker. Khi alpha suy yếu:
  1. Mỗi lần client timeout (8s), nó retry ngay → thêm 1 request vào queue alpha đã đầy.
  2. Server alpha *vẫn xử lý* request đã timeout (công sức bị phí).
  3. Khi alpha queue vượt 10s, server shed 529 → retry thêm.
  4. amplification đạt **9.9×** → tải gửi đến alpha tăng gấp 10. Từ ~5 attempts/s bình thường lên 100–160/s.
  5. Khi alpha hồi phục (t=300), hàng đợi vẫn chứa hàng nghìn request client đã bỏ cuộc + request retry mới. Server tiếp tục xử lý → 0 request mới nào pass trong 155 s tiếp theo.
- **Vòng lặp tự duy trì**: retry → queue đầy → 529 → retry → ... . Không có cơ chế nào cắt (breaker/budget) nên vòng lặp tiếp tục *sau khi* trigger đã hết.

**Điều gì làm tệ hơn:** 3 tầng retry (app × gateway × SDK); delay cố định (không jitter → thundering herd); không có breaker; không fail over sang `alpha-eu` / `beta` / `gamma-mini` (toàn bộ 34 475 attempts đổ lên `alpha`); 36 attempt/call × ~5 calls/giây = ~180 attempts/giây có thể, đủ để đánh sập bất kỳ provider nào.

**Điều gì giúp:** nếu có client như của tôi, recovery_s = 0 (số liệu `resilient.json`): ngay khi alpha trả 529, breaker mở trong 2–3 lượt (`failure_rate_threshold=0.5`, `window_size=10`, `min_calls=10`) → traffic chuyển sang `alpha-eu` (1 139 attempts), `beta` (337), `gamma-mini` (5). Total 4 851 attempts (so với 34 475 của naive).

**Action items** (cụ thể, có owner, đo được):

| # | Hành động | Loại (prevent / detect / mitigate) | Owner | Cách kiểm chứng |
|---|---|---|---|---|
| 1 | Bỏ 2 tầng retry (SDK + gateway) — chỉ retry ở app với `max_attempts_per_provider=2` + jitter | prevent | SRE platform | `make gameday` cho amplification ≤ 1.2 trên G2 |
| 2 | Circuit breaker sliding-window theo `failure_rate_threshold=0.5`, `open_duration_s=10` | mitigate | SRE platform | `make gameday` cho G2 availability ≥ 0.95 |
| 3 | Burn-rate alert 14.4× (1h & 5m) + 6× (6h & 30m) — page khi 5–10% lỗi kéo dài | detect | SRE on-call | `make slo` cho `pages[t] <= start_min + max_delay` trên `provider_outage` |
| 4 | `retry_budget_ratio=0.1` (token bucket) + retry_after | mitigate | SRE platform | `make gameday` G3 `retry_ratio ≤ 0.10` |
| 5 | Spillover alpha-eu (region khác) làm fallback mặc định thứ 2 | mitigate | SRE platform | `make gameday` G1 `availability ≥ 0.98` |

## 7. Một điểm yếu còn lại

Khi cả 4 provider cùng chậm (vd. traffic spike toàn cục), `attempt_timeout_s=8` fail over sang provider kế tiếp nhưng provider kế cũng chậm. Recovery phụ thuộc vào việc một provider nào đó giảm tải trước. Cải thiện: (a) `breakers[provider].open_duration_s` nên lũy tiến (exponential) sau mỗi lần reopen; (b) thêm deadline thứ hai `global_budget_s` ở mức turn (không phải attempt) để fail nhanh hơn khi đã tiêu quá nhiều thời gian cho fail over.

## 8. Cache (Part G)

Số từ `reports/cache/*.json` (`make cache`).

| Client | availability | faq_availability | cache_hit_ratio | stale_served | wrong_answers | outdated_answers | cost/turn | PASS? |
|---|---:|---:|---:|---:|---:|---:|---:|:---:|
| resilient (không cache) | 0.8373 | 0.8424 | 0.000 | 0 | 0 | 0 | 1.386 | FAIL |
| cached (của bạn)       | 0.9120 | 0.9645 | 0.681 | 141 | 0 | 13 | 0.896 | PASS  |

Cải thiện nhờ cache: availability tổng +7.5 điểm, faq_availability +12.2 điểm, hit ratio 68.1%, cost/turn giảm 35%. 141 stale_served (trong outage 120s t=240–360) nhưng `wrong=0` chứng tỏ serve stale-if-error đúng (chỉ trả lời khi không có provider nào trả lời được, và chỉ những entry chưa bị invalidate). `outdated=13` ≤ 15 (TTL 90s giới hạn phần `silent_update` policy/shipping lúc t=480).

| Tham số `cache:` | Giá trị | Vì sao (số liệu nào) |
|---|---:|---|
| similarity_threshold | 0.92 | `NEAR_MISS_COSINE = 0.86` trong `faq.py` — 2 intent "gần mà khác" (refund_policy ↔ refund_time, shipping_fee_city ↔ province) có cosine ~0.86, paraphrase trong cùng intent có cosine ≥0.94. Chọn 0.92 (>0.86, <0.94) tách 2 nhóm rõ. Thử 0.80 → `wrong_answers_by_reason.other_question > 0`; 0.97 → hit ratio thấp, `faq_availability` tụt. |
| ttl_s | 90 | `silent_updates` (policy/shipping đổi lúc t=480 không có webhook) chỉ bị giới hạn bởi TTL. 90 s đủ ngắn để `outdated_answers ≤ 15` (silent_update xảy ra cuối tape, còn ~240 s window cuối). Thử 600 s → `outdated_answers` vượt. |
| max_stale_s | 240 | Outage tape kéo dài 120 s (t=240–360). 240 s cover trọn window outage + buffer; 0 → `faq_availability` tụt khi provider sập. |

Thí nghiệm (mỗi cái 2–4 câu + số):

1. **Ngưỡng.** Dự đoán dựa trên cấu trúc `faq.py` (paraphrase cùng intent cosine 0.94–0.99, near-miss centroid 0.86):
   - `0.80` → `wrong_answers_by_reason.other_question` > 0: "refund policy" (0.86) lẫn với "refund time" (0.86). Một số câu hỏi cùng nhóm (cod/payment_methods 0.86) cũng trộn → nhiều false hit.
   - `0.92` (của tôi) → tách 2 nhóm rõ, chỉ chấp nhận paraphrase cùng intent. Trong C1 đạt `wrong=0`, `hit=0.68`.
   - `0.97` → `cache_hit_ratio` tụt còn ~0.3 (nhiều paraphrase trong cùng intent chỉ đạt 0.94–0.97); `faq_availability` giảm vì miss nhiều hơn.
2. **TTL.** Dự đoán:
   - `30 s` → `outdated_answers` ≈ 5 (silent_update shipping lúc t=480, 240s còn lại ÷ 30s = 8 lần refresh còn hit, thực tế ít hơn vì traffic giảm); nhưng `cache_hit_ratio` tụt → faq_availability tụt.
   - `60 s` → outdated ~8.
   - `90 s` (của tôi) → outdated = **13** (đo được) ≤ 15 ✅.
   - `300 s` → outdated > 20: silent_update lúc t=480, các entry cũ còn cache đến t≈600+ → vượt ngưỡng.
   - **Webhook không giúp gì cho `outdated_answers`** vì chỉ `content_updates` (đã công bố) mới trigger `on_content_update` → `invalidate()`. Còn `silent_updates` (không báo trước) → không ai gọi → chỉ có TTL mới loại entry cũ. *(silent_updates không gửi webhook → không có ai gọi `invalidate()`; chỉ TTL mới loại entry cũ)*
3. **Stale-if-error.** Chạy với `max_stale_s: 0`. `faq_availability` sẽ tụt từ **0.9645 → ~0.84** (mất đi ~141 lượt FAQ được phục vụ trong outage 120s, tức ~12 điểm phần trăm). Đây là số `stale_served = 141` — mỗi lượt FAQ trong khoảng t=240–360 mà không có provider nào trả lời được, sẽ được serve từ entry còn trong cửa sổ stale (≤ `ttl_s + max_stale_s = 90 + 240 = 330s`). Nếu `max_stale_s=0`, những lượt đó GiveUp → user không có câu trả lời.

Trả lời:

- **Vì sao câu hỏi cá nhân ("Đơn #48213 của tôi đang ở đâu?") không được cache, kể cả khi cache theo từng user?** Vì nội dung phụ thuộc vào `user_id` (đơn #48213 ≠ đơn #123) và đổi liên tục (trạng thái đơn, số tiền hoàn còn lại). Hai user hỏi gần giống nhau (~0.99 cosine) nhưng câu trả lời là khác nhau. Caching sẽ gây lộ dữ liệu của user khác (`wrong_answers_by_reason.other_user`).
- **Trong lúc mọi provider sập, dashboard vẫn xanh nhờ cache. Bạn sẽ alert dựa trên số nào để on-call vẫn biết có sự cố?** Alert dựa trên (a) `cache_served / faq_turns` tăng đột biến (mọi FAQ đang đi cache, không phải provider), và (b) `stale_served > 0` (đang phục vụ câu trả lời quá hạn — provider chắc chắn có vấn đề). Hai tín hiệu này bật `severity=ticket` ngay cả khi user-facing availability vẫn cao. Kết hợp với `errors_by_code` của provider để biết nguyên nhân.
