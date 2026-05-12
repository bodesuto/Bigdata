# 📋 Khung Sườn Báo Cáo Cuối Kỳ: Hệ Thống Phát Hiện Gian Lận Real-time

**Tiêu đề:** Xây dựng Pipeline xử lý dữ liệu lớn phát hiện gian lận thời gian thực dựa trên kiến trúc Hybrid Scoring.

---

## 1. Mở Đầu (Introduction)
- **1.1. Bối cảnh:** Sự bùng nổ của giao dịch số và thách thức về gian lận tài chính.
- **1.2. Vấn đề:** Tại sao Batch Processing là không đủ? Sự cần thiết của Real-time.
- **1.3. Mục tiêu:** Xây dựng hệ thống đạt chuẩn Exactly-once, hỗ trợ 3-way join và hybrid scoring.
- **1.4. Cấu trúc báo cáo:** Tóm tắt nội dung các chương.

## 2. Các Công Trình Liên Quan (Related Work)
- **2.1. Rule-based:** Ưu điểm (minh bạch) và Nhược điểm (cứng nhắc).
- **2.2. Machine Learning:** Sức mạnh của Random Forest/XGBoost trong phát hiện mẫu.
- **2.3. Hybrid System:** Tại sao kết hợp Rule + ML là xu hướng của Enterprise.
- **2.4. Công nghệ Streaming:** So sánh Spark Streaming, Flink và Kafka Streams.

## 3. Tổng Quan Dữ Liệu (Dataset Overview)
- **3.1. Dữ liệu PaySim:** Nguồn gốc và phương pháp tạo dữ liệu giả lập.
- **3.2. Schema:** Giải thích ý nghĩa các trường (Amount, OldBalance, NewBalance, v.v.).
- **3.3. Phân tích EDA:** 
    - Biểu đồ tỉ lệ mất cân bằng (Imbalance Ratio: 1:773).
    - Phân tích các loại giao dịch dễ bị gian lận (CASH_OUT, TRANSFER).
- **3.4. Feature Engineering:** Cách tạo ra các đặc trưng mới từ thời gian và số dư.

## 4. Kiến Trúc Hệ Thống (System Architecture)
- **4.1. Thiết kế tổng thể:** *[Chèn sơ đồ Mermaid từ README vào đây]*.
- **4.2. Thành phần công nghệ:** Tại sao chọn Kafka (Buffer), Spark (Compute), Cassandra (Storage), Redis (Cache).
- **4.3. Luồng dữ liệu:** Hành trình từ CSV -> Kafka -> Spark -> Sinks.
- **4.4. Thiết kế 3 nguồn độc lập:** Lý do mô phỏng 3 hệ thống ngân hàng riêng biệt để tăng tính thực tế.
- **4.5. Triển khai Docker:** Danh sách 10 dịch vụ và cách chúng kết nối.
- **4.6. Giám sát hệ thống:** Vai trò của Grafana và Prometheus.
- **4.7. Tính nhất quán dữ liệu (Data Consistency):** Cách đảm bảo dữ liệu từ 3 nguồn độc lập khớp nối chính xác thông qua `source_event_id`.

## 5. Công Cụ Thực Thi Luật (Rule Engine)
- **5.1. Các quy tắc nghiệp vụ:** Giải thích 6 luật (Account Drain, Fan-out, v.v.).
- **5.2. Thuật toán chấm điểm rủi ro:** Công thức tính điểm tổng hợp từ nhiều luật.
- **5.3. Cấu hình động:** Cách thay đổi luật thông qua Kafka mà không cần khởi động lại Spark.

## 6. Mô Hình Máy Học (Machine Learning Model)
- **6.1. Tiền xử lý:** Chia tập dữ liệu theo thời gian (Time-based split).
- **6.2. Xử lý mất cân bằng:** Kỹ thuật SMOTE và Class Weight.
- **6.3. Huấn luyện:** Tại sao chọn Random Forest? Các tham số tối ưu.
- **6.4. Đánh giá Threshold:** Cách chọn ngưỡng 0.8445 để tối ưu F1-Score.
- **6.5. Tầm quan trọng của đặc trưng:** Biểu đồ Feature Importance.

## 7. Kết Quả Đánh Giá (Evaluation Results)
- **7.1. Chỉ số đo lường:** Precision, Recall, F1, AUC-ROC.
- **7.2. Confusion Matrix:** Phân tích các ca False Negative và False Positive.
- **7.3. Biểu đồ đường cong:** PR Curve và ROC Curve.

## 8. Tích Hợp Hybrid Scoring & Streaming
- **8.1. Công thức kết hợp:** `0.6 * Rule + 0.4 * ML`.
- **8.2. Kỹ thuật Interval Join:** Cách Spark ghép nối 3 luồng dữ liệu bất đồng bộ.
- **8.3. Xử lý Watermarking:** Cách hệ thống tự dọn dẹp RAM.

## 9. Kết Quả Vận Hành Pipeline (Streaming Pipeline Results)
- **9.1. Thông lượng (Throughput):** EPS thực tế đo được trên Grafana.
- **9.2. Độ trễ (Latency):** Thời gian xử lý trung bình mỗi mẻ (Batch duration).
- **9.3. Exactly-once:** Chứng minh tính toàn vẹn dữ liệu qua Checkpointing.
- **9.4. Tối ưu hóa:** Xử lý Data Skew và Latency Reduction.
- **9.5. Phân tích thực nghiệm:** So sánh hiệu năng giữa các tham số Trigger khác nhau (Default vs 2s vs 10s).

## 10. Thảo Luận & Đánh Giá (Discussion)
- **10.1. Ưu điểm:** Tính ổn định, khả năng quan sát tốt.
- **10.2. Hạn chế:** Phụ thuộc vào dữ liệu giả lập, độ phức tạp của môi trường Docker.
- **10.3. Đánh đổi:** Cân bằng giữa tốc độ xử lý và độ chính xác của Model.
- **10.4. Bảo mật & Quyền riêng tư:** Cách hệ thống xử lý ẩn danh dữ liệu khách hàng (Anonymization) và bảo vệ Kafka/Cassandra.
- **10.5. Đạo đức & Định kiến (Ethical Considerations):** Phân tích tác động của việc nhầm lẫn (False Positives) đối với trải nghiệm khách hàng.

## 11. Kết Luận & Hướng Phát Triển (Conclusion)
- **11.1. Tổng kết:** Những gì dự án đã đạt được so với mục tiêu đề ra.
- **11.2. Hướng phát triển:** Tích hợp Deep Learning, cập nhật Model real-time (Online Learning).

---
## Phụ Lục (Appendix)
- **A. Data Dictionary:** *(Lấy từ file DATA_DICTIONARY.md)*.
- **B. Handover Guide:** *(Lấy từ file HANDOVER_GUIDE.md)*.
- **C. Screenshots:** Ảnh chụp Dashboard thực tế.
