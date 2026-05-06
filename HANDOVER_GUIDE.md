# 📘 Tài Liệu Bàn Giao Hệ Thống Phát Hiện Gian Lận Real-time (v4)

**Dự án:** Enterprise Fraud Detection Pipeline  
**Phiên bản:** 4.0.0 (Production-Ready)  
**Tình trạng:** Hoàn thiện 100%  

---

## 🏗️ 1. Kiến Trúc Hệ Thống (Architecture Specification)

Hệ thống được xây dựng trên mô hình **EDA (Event-Driven Architecture)** đa tầng, đảm bảo tính mở rộng và khả năng chịu lỗi cực cao.

```mermaid
graph TD
    subgraph "Data Sources (Heterogeneous)"
        A[Transaction CSV]
        B[Sender State CSV]
        C[Receiver State CSV]
    end

    subgraph "Ingestion Layer (Kafka)"
        D[Kafka Broker: 9092]
        E[Parallel Producers]
        A & B & C --> E --> D
    end

    subgraph "Processing Layer (Spark Streaming)"
        F[Spark Driver]
        G[Spark Workers]
        D --> F
        F --> G
        Note right of F: 3-Way Interval Join<br/>Rule Engine & ML Scoring
    end

    subgraph "Storage & Serving Layer"
        H[(Cassandra: Alert History)]
        I[(Redis: Hot Cache)]
        J[Kafka Alert Topic]
        G --> H & I & J
    end

    subgraph "Observability Layer"
        K[Grafana: Metrics Dashboard]
        L[Streamlit: Fraud Command Center]
        H & I --> L
        G --> M[Prometheus] --> K
    end
```

---

## 🧠 2. Giải Thích Kỹ Thuật Chuyên Sâu (Technical Deep Dive)

### 2.1. Phép nối 3 luồng (3-Way Interval Join)
Đây là "trái tim" của hệ thống. Spark không chỉ đọc dữ liệu, nó thực hiện ghép nối 3 luồng tin nhắn bất đồng bộ từ Kafka.
- **Cơ chế:** Sử dụng `Interval Join` với ngưỡng trễ (Tolerance) 30 giây.
- **Ý nghĩa:** Đảm bảo dù tin nhắn về số dư đến trước hay sau giao dịch, hệ thống vẫn ghép đúng "bức tranh toàn cảnh" để bắt gian lận.

### 2.2. Tính nhất quán Exactly-once
Hệ thống cam kết dữ liệu không bị mất và không bị nhân đôi (Idempotency).
- **Kỹ thuật:** Sử dụng `ForeachBatch` kết hợp với bảng `processed_stream_batches` trong Cassandra. Trước khi ghi dữ liệu, Spark kiểm tra ID của lô (Batch ID), nếu đã tồn tại thì sẽ bỏ qua.

### 2.3. Quản lý trạng thái (State Management)
- **Watermarking (10 phút):** Tự động dọn dẹp các bản ghi cũ trong RAM sau 10 phút để tránh lỗi tràn bộ nhớ (Out of Memory).
- **Redis Caching:** Lưu trữ 100 giao dịch gần nhất của mỗi tài khoản để tính toán tần suất (Velocity Check) trong thời gian thực.

---

## 🚀 3. Hướng Dẫn Vận Hành (Operational Manual)

### 3.1. Quy trình khởi động tiêu chuẩn (Standard Boot Procedure)
1. **Khởi động hạ tầng:** `docker-compose up -d`
2. **Kiểm tra trạng thái:** Đảm bảo tất cả 7 Container trong Docker Desktop hiện màu xanh.
3. **Khởi tạo dữ liệu nguồn:** Chạy `scripts/split_logical_sources.py` (Mất ~1 phút).
4. **Bootstrap hệ thống:** Chạy `scripts/bootstrap_local_stack.py` để tạo bảng và nạp Risk Rules.
5. **Kích hoạt luồng:** Chạy `scripts/publish_logical_sources_parallel.py --rate 100`.

### 3.2. Thông số giám sát (Monitoring KPIs)
| Chỉ số | Vị trí kiểm tra | Ngưỡng an toàn |
| :--- | :--- | :--- |
| **Throughput (EPS)** | Grafana | > 100 events/sec |
| **Batch Latency** | Grafana | < 3.0 seconds |
| **Memory Usage** | Grafana | < 2GB JVM Heap |
| **Live Alerts** | Streamlit | Theo dõi các dòng 🔴 CRITICAL |

---

## 📂 4. Danh Mục Bàn Giao (Handover Checklist)

| Thành phần | Đường dẫn/Cổng | Mô tả |
| :--- | :--- | :--- |
| **Mã nguồn lõi** | `/fraud_pipeline` | Chứa logic Rule Engine, ML và Serialization. |
| **Spark Application** | `/spark-app` | Chứa `stream_job.py` - bộ não thực thi. |
| **Cơ sở dữ liệu** | Cassandra (9042), Redis (6379) | Lưu trữ cảnh báo và trạng thái nóng. |
| **Công cụ Dashboard** | Streamlit (8501), Grafana (3001) | Giao diện vận hành và giám sát kỹ thuật. |
| **Tài liệu hướng dẫn** | `README.md`, `USER_MANUAL.md` | Hướng dẫn cài đặt và sử dụng chi tiết. |

---

## 🛠️ 5. Bảo Trì & Nâng Cấp (Maintenance)

- **Thêm luật mới:** Chỉnh sửa file `fraud_pipeline/rules.py` và nạp lại vào Kafka thông qua `publish_risk_rules.py`.
- **Mở rộng hệ thống:** Tăng số lượng `replicas` cho Spark Worker trong file `docker-compose.yml`.
- **Lưu trữ dữ liệu:** Cassandra tự động phân vùng theo ngày, bạn có thể thiết lập TTL (Time-to-Live) để tự động xóa dữ liệu cũ sau 30 ngày.

---
**Người bàn giao:** Antigravity AI Assistant  
**Ngày bàn giao:** 06/05/2026  
*Hệ thống đã sẵn sàng cho môi trường Production.*
