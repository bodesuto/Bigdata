# 📖 Từ Điển Dữ Liệu Hệ Thống (Data Dictionary)

Tài liệu này định nghĩa chi tiết các thực thể dữ liệu trong hệ thống phát hiện gian lận.

---

## 1. Cassandra Schema (Tầng lưu trữ vĩnh viễn)

### 1.1. Bảng `alerts_by_account`
Lưu trữ thông tin chi tiết về các giao dịch bị nghi ngờ gian lận.

| Tên cột | Kiểu dữ liệu | Ý nghĩa |
| :--- | :--- | :--- |
| `account_id` | `text` | ID của tài khoản thực hiện giao dịch (Partition Key). |
| `alert_ts` | `timestamp` | Thời điểm phát hiện gian lận (Clustering Column). |
| `alert_id` | `uuid` | Mã định danh duy nhất của cảnh báo. |
| `txn_type` | `text` | Loại giao dịch (CASH_OUT, TRANSFER, v.v.). |
| `amount` | `double` | Số tiền giao dịch. |
| `risk_score` | `double` | Điểm rủi ro từ Rule Engine (0.0 - 1.0). |
| `ml_score` | `double` | Điểm rủi ro từ Mô hình máy học (0.0 - 1.0). |
| `severity` | `text` | Mức độ nghiêm trọng (high, medium, low). |
| `triggered_rules` | `list<text>` | Danh sách các quy tắc đã bị vi phạm. |

### 1.2. Bảng `metrics_by_window`
Lưu trữ các thông số kỹ thuật phục vụ biểu đồ Grafana.

| Tên cột | Kiểu dữ liệu | Ý nghĩa |
| :--- | :--- | :--- |
| `window_type` | `text` | Loại cửa sổ (sliding hoặc tumbling). |
| `window_start` | `timestamp` | Thời điểm bắt đầu của khung giờ. |
| `event_count` | `bigint` | Tổng số giao dịch trong khung giờ đó. |
| `fraud_rate` | `double` | Tỉ lệ gian lận phát hiện được. |

---

## 2. Kafka Topics (Tầng vận chuyển)

| Topic Name | Producer | Consumer | Ý nghĩa |
| :--- | :--- | :--- | :--- |
| `transaction_topic` | Producer TX | Spark Job | Luồng dữ liệu giao dịch thô. |
| `sender_state_topic` | Producer Sender | Spark Job | Luồng trạng thái số dư người gửi. |
| `fraud_alerts` | Spark Job | Dashboard | Luồng các cảnh báo đã được xử lý. |
| `pipeline_dead_letter` | Spark Job | Admin | Nơi chứa các bản ghi lỗi định dạng. |

---

## 3. Redis Keys (Tầng Cache)

| Key Pattern | TTL | Ý nghĩa |
| :--- | :--- | :--- |
| `fraud_alert:<alert_id>` | 24h | Lưu Payload của cảnh báo để Dashboard hiển thị nhanh. |
| `published_alert:<run_id>:<id>` | 24h | Đánh dấu Alert đã được gửi đi để chống trùng lặp. |
| `sender_history:<account_id>` | 1h | Danh sách các giao dịch gần đây để tính toán vận tốc giao dịch. |

---
*Tài liệu này giúp đảm bảo sự nhất quán trong việc hiểu và khai thác dữ liệu của hệ thống.*
