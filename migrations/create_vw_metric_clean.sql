-- 创建清洗后的指标视图
-- 执行日期: 2026-04-15

CREATE OR REPLACE VIEW vw_metric_clean AS
SELECT 
    id,
    news_id,
    title,
    publish_date,
    year,
    month,
    metric_key,
    metric_name,
    metric_value,
    metric_raw,
    source_table,
    created_at,
    updated_at
FROM health_ocr_metrics
WHERE year IS NOT NULL
  AND metric_value IS NOT NULL
  AND (
    (metric_key = 'doctor_count' AND metric_value BETWEEN 1000 AND 300000)
    OR (metric_key = 'nurse_count' AND metric_value BETWEEN 1000 AND 400000)
    OR (metric_key = 'bed_count' AND metric_value BETWEEN 1000 AND 600000)
    OR (metric_key = 'bed_usage_rate' AND metric_value BETWEEN 1 AND 100)
    OR (metric_key = 'outpatient_visits' AND metric_value BETWEEN 100000 AND 50000000)
    OR (metric_key = 'discharge_count' AND metric_value BETWEEN 1000 AND 5000000)
    OR (metric_key = 'avg_stay_days' AND metric_value BETWEEN 1 AND 30)
    OR (metric_key = 'outpatient_cost' AND metric_value BETWEEN 1 AND 2000)
    OR (metric_key = 'discharge_cost' AND metric_value BETWEEN 1 AND 50000)
  );
