-- OCR结构化指标表
-- 执行日期: 2026-04-10

CREATE TABLE IF NOT EXISTS health_ocr_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    news_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    publish_date VARCHAR(50),
    year INT,
    month INT,
    metric_key VARCHAR(64) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(18, 4),
    metric_raw VARCHAR(64),
    source_table VARCHAR(32) NOT NULL DEFAULT 'guangxi_news',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_news_metric (news_id, metric_key),
    KEY idx_year_month (year, month),
    KEY idx_metric_key (metric_key)
);
