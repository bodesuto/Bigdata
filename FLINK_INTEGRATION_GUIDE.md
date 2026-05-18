# WHITE PAPER: Cấu trúc Xử lý & Tích hợp Dữ liệu Lớn Thời gian thực với Apache Flink

## 1. Tổng quan Hệ thống (System Overview)
Hệ thống được thiết kế để giải quyết bài toán phát hiện gian lận tài chính ở quy mô **Big Data**. Mục tiêu cốt lõi là nhận diện các hành vi bất thường từ dòng dữ liệu giao dịch khổng lồ với độ trễ tối thiểu (Milliseconds) và độ chính xác tối đa.

---

## 2. Các Cột trụ Nguyên lý xử lý Big Data

### 2.1. Nguyên lý Vận tốc (Velocity) - xử lý Native Streaming
Thay vì sử dụng cơ chế Micro-batch (gom lô nhỏ) như Spark, Flink vận hành theo nguyên lý **Continuous Streaming**.
*   **Cơ chế:** Mỗi bản ghi là một đơn vị xử lý độc lập. 
*   **Lợi ích:** Loại bỏ hoàn toàn sự lãng phí thời gian chờ đợi Micro-batch. Điều này cực kỳ quan trọng trong gian lận tài chính, nơi mỗi giây chậm trễ có thể dẫn đến thất thoát hàng tỷ đồng.

### 2.2. Nguyên lý Thời gian sự kiện (Event-Time) & Watermarking
Đây là chìa khóa để đảm bảo tính **Chính xác (Accuracy)** trong tích hợp dữ liệu.
*   **Nguyên lý:** Hệ thống không dùng thời gian của máy chủ (System Time) mà dùng thời gian thực sự xảy ra giao dịch (Event Time).
*   **Watermarking:** Là cơ chế "Dấu thời gian trễ". Nếu một dữ liệu bị đến muộn do sự cố mạng, Flink sẽ sử dụng Watermark để "đợi" dữ liệu đó, đảm bảo phép Join không bị sai lệch.

### 2.3. Nguyên lý Lưu trữ Trạng thái (Managed Stateful Processing)
Để Join 3 luồng dữ liệu, Flink phải "nhớ" các sự kiện đã qua.
*   **Cơ chế:** Flink lưu trữ thông tin số dư (Sender/Receiver) vào **Managed State** (một dạng bộ nhớ cache phân tán được quản lý cực kỳ nghiêm ngặt).
*   **Hiệu năng:** Việc lưu trữ này cho phép truy xuất thông tin với độ trễ gần như bằng 0, thay vì phải truy vấn vào Database truyền thống.

---

## 3. Quy trình Tích hợp & Xử lý Step-by-Step

### Bước 1: Ingestion & Decoupling (Tiếp nhận và Tách lớp)
Dữ liệu thô từ hệ thống Core-Banking được đẩy vào **Kafka**. 
*   **Nguyên lý:** Kafka đóng vai trò là "Cửa ngõ" để bảo vệ các hệ thống xử lý phía sau khỏi hiện tượng "Spike" (Dữ liệu tăng đột biến), đảm bảo hệ thống không bao giờ bị quá tải.

### Bước 2: Temporal 3-Way Join (Tích hợp luồng đa chiều)
Đây là giai đoạn phức tạp nhất, thực hiện **tích hợp dữ liệu (Data Integration)**:
*   **Nghiệp vụ:** Một giao dịch không thể đứng độc lập. Nó cần biết: "Số dư người gửi có đủ không?" và "Người nhận là ai?".
*   **Nguyên lý Join:** Flink thực hiện phép **Interval Join** trên 3 bảng SQL ảo. Flink sẽ giữ bản ghi Transaction và tìm kiếm mảnh ghép Sender/Receiver trong cửa sổ 30 giây. 
*   **Tích hợp:** Kết quả là một "Siêu bản ghi" (Enriched Record) chứa đầy đủ thông tin của cả 3 nguồn.

### Bước 3: Hybrid Scoring Logic (Xử lý nghiệp vụ hỗn hợp)
Tận dụng sức mạnh của cả **Luật cứng (Hard Rules)** và **Trí tuệ nhân tạo (AI)**:
*   **Logic Nghiệp vụ:** Rule Engine thực hiện các kiểm tra nhanh (ví dụ: chuyển tiền vượt số dư, chuyển tiền vào giờ lạ).
*   **Logic ML:** Mô hình Machine Learning tính toán xác suất gian lận dựa trên các đặc điểm ẩn mà luật cứng không thấy được.
*   **Nguyên lý:** Xử lý song song để ra quyết định cuối cùng trong vòng chưa đầy 10ms.

### Bước 4: Persistence & Dual-Speed Storage (Lưu trữ đa tầng)
Dữ liệu sau xử lý được đẩy vào 2 kho lưu trữ với mục đích khác nhau:
1.  **Cassandra (Long-term):** Lưu trữ theo nguyên lý **LSM-Tree**, tối ưu cho việc ghi dữ liệu Big Data tốc độ cao. Phục vụ cho việc truy xuất lịch sử, đối soát và báo cáo định kỳ.
2.  **Redis (Hot-store):** Lưu trữ theo nguyên lý **In-memory Key-Value**. Dùng để phục vụ Dashboard và các hệ thống cảnh báo tức thời, đảm bảo người dùng thấy kết quả ngay khi nó vừa được xử lý xong.

---

## 4. Đảm bảo tính Tin cậy trong Production

### Cơ chế Exactly-Once (Chính xác đúng một lần)
Sử dụng thuật toán **Chandy-Lamport distributed snapshot**:
*   Flink định kỳ tạo các ranh giới (Barriers) trong luồng dữ liệu. Khi các ranh giới này đi qua toàn bộ hệ thống và được ghi lại, đó là một **Checkpoint**.
*   **Ý nghĩa:** Nếu hệ thống sập, nó sẽ tự động "rollback" về checkpoint gần nhất và xử lý lại các dữ liệu chưa hoàn tất, đảm bảo không có giao dịch nào bị xử lý 2 lần (gây sai lệch số dư) hoặc bị bỏ sót hoàn toàn.

### Khả năng Mở rộng (Scalability)
Cấu trúc Flink cho phép chia nhỏ luồng dữ liệu thành hàng nghìn "Keyed-Streams". Mỗi cụm TaskManager sẽ xử lý một phần dữ liệu, cho phép hệ thống đáp ứng lượng giao dịch của một ngân hàng cấp quốc gia.

---

## 5. Kết luận
Hệ thống không chỉ đáp ứng về mặt **Kỹ thuật** (Tốc độ, Lưu trữ) mà còn giải quyết triệt để bài toán **Nghiệp vụ** (Phát hiện gian lận phức tạp). Việc sử dụng Apache Flink làm hạt nhân giúp hệ thống trở thành một giải pháp **Big Data Real-time** tiêu chuẩn, mạnh mẽ và sẵn sàng cho môi trường Production thực tế.
