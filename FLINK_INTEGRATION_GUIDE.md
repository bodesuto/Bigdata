# PHÂN TÍCH CHUYÊN SÂU KIẾN TRÚC TÍCH HỢP & XỬ LÝ BIG DATA REAL-TIME (APACHE FLINK)

## 1. GIỚI THIỆU (INTRODUCTION)
Hệ thống phát hiện gian lận tài chính này được thiết kế theo kiến trúc **Cloud-Native Big Data**, lấy Apache Flink làm hạt nhân xử lý. Khác với các hệ thống truyền thống dựa trên cơ sở dữ liệu quan hệ, hệ thống này tích hợp dữ liệu dòng (Streaming Data) để phản ứng với hành vi gian lận ngay tại thời điểm nó phát sinh.

---

## 2. KIẾN TRÚC TÍCH HỢP HỆ THỐNG (INTEGRATION ARCHITECTURE)

### 2.1. Tầng Thu thập (Ingestion Layer) - Apache Kafka
*   **Nguyên lý:** Sử dụng mô hình **Publish/Subscribe**. Kafka đóng vai trò là cột xương sống (Backbone) lưu trữ log bất biến.
*   **Chi tiết Tích hợp:** Dữ liệu được chia thành 3 Topics chuyên biệt (`transactions`, `sender_state`, `receiver_state`). Việc phân tách này cho phép hệ thống đạt được tính **Scalability ngang** (mở rộng bằng cách thêm Partitions).
*   **Tác dụng:** Chống chịu được các đợt bùng nổ dữ liệu (Traffic Spikes) mà không làm mất thông tin.

### 2.2. Tầng Xử lý Trung tâm (Processing Layer) - Apache Flink
Đây là nơi diễn ra sự hội tụ của 3 nguyên lý Big Data hiện đại:
1.  **Temporal Join:** Khớp nối thông tin người dùng với giao dịch trong thời gian thực.
2.  **Analytical Windowing:** Tính toán các chỉ số thống kê (Tumbling Windows) để nhận diện xu hướng gian lận.
3.  **AI/ML Integration:** Chạy các mô hình máy học trực tiếp trên luồng dữ liệu (In-stream Inference).

---

## 3. CƠ CHẾ XỬ LÝ DỮ LIỆU CHUYÊN SÂU

### 3.1. Nguyên lý Watermarking & Xử lý Dữ liệu đến muộn (Late Data)
Trong môi trường mạng phân tán, dữ liệu về số dư người gửi có thể đến sau giao dịch 1-2 giây.
*   **Xử lý trong Flink:** Sử dụng **Watermark strategy with Idleness detection**. 
*   **Logic:** Watermark là một dấu hiệu cho Flink biết "Không còn dữ liệu nào trước thời điểm T này nữa". Nếu dữ liệu đến muộn trong phạm vi 30 giây (Join Window), Flink sẽ tự động gộp nó vào đúng phiên làm việc.

### 3.2. Quản lý Trạng thái (Managed State & RocksDB)
Flink lưu trữ thông tin tạm thời (như các bản ghi đang chờ để Join) vào **State Backend**.
*   **Nguyên lý:** Sử dụng **Keyed State**. Dữ liệu được băm (Hash) theo `event_id` để phân phối đều trên tất cả các TaskManagers.
*   **State TTL (Time-To-Live):** Để tránh việc RAM bị đầy, tôi đã cấu hình State chỉ tồn tại trong 30 giây. Sau thời gian này, các phần dữ liệu không có mảnh ghép tương ứng sẽ được tự động dọn dẹp để giải phóng bộ nhớ.

### 3.3. Cơ chế Phản hồi ngược (Backpressure Management)
Đây là đặc tính cao cấp giúp Flink vượt trội hơn các hệ thống khác.
*   **Nguyên lý:** Nếu Cassandra hoặc tầng Python xử lý chậm, Flink sẽ tự động điều tiết tốc độ đọc từ Kafka xuống. Việc này giúp hệ thống **không bao giờ bị tràn bộ nhớ** dẫn đến sụp đổ dây chuyền (Cascading Failure).

---

## 4. TÍCH HỢP ĐA TẦNG LƯU TRỮ (STORAGE INTEGRATION STRATEGY)

Hệ thống áp dụng chiến lược **Polyglot Persistence** (Sử dụng đúng cơ sở dữ liệu cho đúng mục đích):

1.  **Redis (Hot Storage - Tầng nóng):** 
    *   **Mục đích:** Cảnh báo tức thời và Dashboard thời gian thực.
    *   **Nguyên lý:** Lưu trữ dưới dạng Key-Value trong RAM. Tốc độ truy xuất < 1ms.
2.  **Cassandra (Cold Storage - Tầng lạnh):**
    *   **Mục đích:** Lưu trữ lịch sử giao dịch khổng lồ cho mục đích hậu kiểm (Audit) và huấn luyện lại AI.
    *   **Nguyên lý:** Kiến trúc **LSM-Tree** giúp Cassandra ghi dữ liệu với tốc độ hàng trăm nghìn bản ghi/giây mà không bị khóa bảng (Table Locking) như SQL Server hay MySQL.

---

## 5. BẢO VỆ TÍCH HỢP & TÍNH CHÍNH XÁC (EXACTLY-ONCE GUARANTEE)

Để đảm bảo tính đúng đắn trong ngân hàng (không thể trừ tiền của khách 2 lần):
*   **Barrier-based Snapshotting:** Flink chèn các dấu "Barriers" vào luồng dữ liệu Kafka. Khi tất cả các ranh giới này hoàn tất chu trình qua hệ thống và ghi xuống Disk thành công, hệ thống đạt trạng thái **Consitency**.
*   **Idempotent Writes:** Khi ghi vào Cassandra, chúng ta sử dụng `event_id` làm Primary Key. Dù Flink có ghi 1 bản ghi 2 lần (sau khi hồi phục lỗi), Cassandra cũng sẽ tự động ghi đè (Upsert) thay vì tạo bản ghi mới. Điều này đảm bảo tính **Nhất quán** tuyệt đối.

---

## 6. SO SÁNH PHÂN TÍCH (FLINK VS. SPARK)

| Tiêu chí | Spark Structured Streaming | Apache Flink (Native) |
| :--- | :--- | :--- |
| **Độ trễ (Latency)** | 2s - 20s (Do Micro-batch) | **< 100ms (Event-driven)** |
| **Quản lý State** | Cồng kềnh, khó dọn dẹp | **Tối ưu, có cơ chế TTL tự động** |
| **Join 3 luồng** | Phức tạp, dễ lỗi đồng bộ | **Mượt mà qua SQL Temporal Join** |
| **Khả năng Scaling** | Ổn định | **Vượt trội với Fine-grained scaling** |

---

## 7. KẾT LUẬN
Hệ thống phát hiện gian lận dựa trên Apache Flink này không chỉ dừng lại ở mức mô hình thử nghiệm. Với việc áp dụng các nguyên lý tiên tiến về **Xử lý dòng có trạng thái**, **Phản hồi ngược**, và **Nhất quán dữ liệu**, đây là một giải pháp kiến trúc Big Data hoàn chỉnh, mạnh mẽ và có độ tin cậy cực cao dành cho môi trường tài chính hiện đại.
