# Reflection

**Kết quả:** practice 50,0 (bắt 33/33 lỗi, 0 báo nhầm, chi phí 180/220) · public 45,1 (TPR 0,95, FPR 0,017, chi phí 240/220, cả 4 pillar band `high`) · **private 31,1 (TPR 0,63, FPR 0,014, không vượt ngân sách 320)**. Điểm tối đa của công thức là 50.

## Fault nào khó bắt nhất, vì sao?

1. **Lỗi subtle (độ lớn sát dao động bình thường).** Đây là nguyên nhân chính khiến TPR rớt từ 0,95 (public) xuống 0,63 (private). Baseline chỉ là mean ± 3σ nên một lỗi nằm ở khoảng 1,5–2,5σ gần như không phân biệt được với một event sạch ở cái đuôi của phân phối. Tôi chỉ siết ngưỡng xuống khoảng 2,5σ (hệ số 0,85 cho chỉ số hai phía, 0,9 cho chỉ số một phía) nên lỗi nhỏ hơn mức đó vẫn lọt. Harness không trả số liệu theo pillar ở private nên tôi không biết chắc pillar nào hụt nhiều nhất; theo suy đoán thì `checks` (4 chỉ số, mỗi chỉ số một ngưỡng) và `ai_infra` (feature skew nhỏ) là nơi dễ có lỗi sát ngưỡng nhất, nhưng đây chỉ là giả thuyết chưa kiểm chứng.
2. **Lineage không có "đáp án mong đợi" trong payload.** Payload chỉ khai báo input `raw.orders`, trong khi upstream thật của run sạch là `raw.orders` + `raw.customers`. Nếu so với payload thì mọi run đều "đúng", còn nếu hardcode tập upstream thì dễ vỡ khi dữ liệu đổi. Tôi giải quyết bằng cách học tập upstream/số downstream phổ biến nhất của từng job trong `ctx.state` rồi báo khi thiếu, và học median thời gian chạy từ các lần chạy bình thường để siết ngưỡng runtime. Cách này phải chờ vài mẫu đầu mới ổn định, nên lỗi nằm ngay đầu stream có thể lọt.
3. **Contract vi phạm SLA nhẹ.** `contract_diff` chỉ trả `violations` cho schema/kiểu; độ trễ chỉ là một con số nên tôi phải tự so với ngưỡng, và lỗi độ trễ nhẹ có cùng vấn đề với mục 1.

Một bẫy kỹ thuật đáng nhớ: `import statistics` nằm trong allowlist nhưng module này nạp ngầm `numbers` nên sandbox chặn; tôi phải tự viết hàm median.

## Nếu có thêm một lượt, tôi sẽ đổi gì ở cân bằng cost/coverage?

- **Công thức nghiêng về coverage.** Bắt đúng một lỗi thật cộng 0,5/số_lỗi, còn một báo nhầm chỉ trừ 0,3/số_event_sạch; với tỉ lệ lỗi khoảng 25% thì một lần bắt đúng bù được khoảng 4 lần báo nhầm. Vì vậy ngưỡng tối ưu nên siết hơn nữa, có thể xuống 2σ, nhất là khi private nặng subtle. Tôi dừng ở 2,5σ vì chỉ có practice/public để hiệu chỉnh và không muốn khớp ngưỡng theo một stream (đúng tinh thần anti-overfit), nhưng kết quả private cho thấy tôi đã quá thận trọng.
- **Chi phí không phải nút thắt.** Gọi mỗi tool một lần cho mỗi event (public tốn 240 so với ngân sách 220) chỉ bị trừ khoảng 1,8 điểm cho 9% vượt ngân sách, trong khi bỏ qua một lời gọi 2 credit làm mất kỳ vọng nhiều điểm hơn; ở private tổng chi phí nằm trong ngân sách nên không bị trừ gì. Không có tín hiệu rẻ nào trong payload (feature/embedding không có trường nào để lọc trước) nên tôi không sàng lọc event trước khi gọi tool.
- **Kết hợp tín hiệu mạnh hơn.** Hiện tôi chỉ kết hợp ở embedding (cả centroid và tuổi tài liệu cùng sát ngưỡng) và dùng chỉ số độc lập ở nơi khác. Với `checks` có thể dùng điểm tổng hợp (ví dụ tổng bình phương z của 4 chỉ số) và theo dõi xu hướng liên tiếp qua các batch để bắt lỗi nhỏ mà kéo dài, thay vì chấm từng event riêng lẻ.
