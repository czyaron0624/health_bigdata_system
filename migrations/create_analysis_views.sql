-- ================================================================
-- 六大模块分析视图和表创建脚本
-- 用途: 创建六大基础模块所需的数据库视图和分析结果表
-- 执行: mysql -u root -p health_db < migrations/create_analysis_views.sql
-- ================================================================

-- 1. 人口统计视图 (population_info 表已有 region 字段)
CREATE OR REPLACE VIEW v_population_stats AS
SELECT 
    region,
    age_group,
    gender,
    SUM(population_count) as total_population,
    COUNT(*) as record_count
FROM population_info
GROUP BY region, age_group, gender;

-- 2. 机构统计视图 (medical_institution 表已有 region 字段)
CREATE OR REPLACE VIEW v_institution_stats AS
SELECT 
    region,
    type,
    level,
    COUNT(*) as institution_count
FROM medical_institution
GROUP BY region, type, level;

-- 3. 人员统计视图 (根据 source_table 推断地区)
CREATE OR REPLACE VIEW v_personnel_stats AS
SELECT 
    CASE 
        WHEN source_table = 'guangxi_news' THEN '广西'
        WHEN source_table = 'sichuan_news' THEN '四川'
        WHEN source_table = 'national_news' THEN '国家'
        ELSE source_table
    END as region,
    year,
    SUM(CASE WHEN metric_key = 'doctor_count' THEN metric_value ELSE 0 END) as doctor_count,
    SUM(CASE WHEN metric_key = 'nurse_count' THEN metric_value ELSE 0 END) as nurse_count
FROM health_ocr_metrics
WHERE metric_key IN ('doctor_count', 'nurse_count')
GROUP BY source_table, year;

-- 4. 床位统计视图
CREATE OR REPLACE VIEW v_bed_stats AS
SELECT 
    CASE 
        WHEN source_table = 'guangxi_news' THEN '广西'
        WHEN source_table = 'sichuan_news' THEN '四川'
        WHEN source_table = 'national_news' THEN '国家'
        ELSE source_table
    END as region,
    year,
    SUM(CASE WHEN metric_key = 'bed_count' THEN metric_value ELSE 0 END) as bed_count,
    AVG(CASE WHEN metric_key = 'bed_usage_rate' THEN metric_value ELSE NULL END) as avg_usage_rate
FROM health_ocr_metrics
WHERE metric_key IN ('bed_count', 'bed_usage_rate')
GROUP BY source_table, year;

-- 5. 服务统计视图
CREATE OR REPLACE VIEW v_service_stats AS
SELECT 
    CASE 
        WHEN source_table = 'guangxi_news' THEN '广西'
        WHEN source_table = 'sichuan_news' THEN '四川'
        WHEN source_table = 'national_news' THEN '国家'
        ELSE source_table
    END as region,
    year,
    SUM(CASE WHEN metric_key = 'outpatient_visits' THEN metric_value ELSE 0 END) as outpatient_visits,
    SUM(CASE WHEN metric_key = 'discharge_count' THEN metric_value ELSE 0 END) as discharge_count,
    AVG(CASE WHEN metric_key = 'avg_stay_days' THEN metric_value ELSE NULL END) as avg_stay_days
FROM health_ocr_metrics
WHERE metric_key IN ('outpatient_visits', 'discharge_count', 'avg_stay_days')
GROUP BY source_table, year;

-- 6. 费用统计视图
CREATE OR REPLACE VIEW v_cost_stats AS
SELECT 
    CASE 
        WHEN source_table = 'guangxi_news' THEN '广西'
        WHEN source_table = 'sichuan_news' THEN '四川'
        WHEN source_table = 'national_news' THEN '国家'
        ELSE source_table
    END as region,
    year,
    AVG(CASE WHEN metric_key = 'outpatient_cost' THEN metric_value ELSE NULL END) as avg_outpatient_cost,
    AVG(CASE WHEN metric_key = 'discharge_cost' THEN metric_value ELSE NULL END) as avg_discharge_cost
FROM health_ocr_metrics
WHERE metric_key IN ('outpatient_cost', 'discharge_cost')
GROUP BY source_table, year;

-- 7. 综合指标汇总视图
CREATE OR REPLACE VIEW v_health_metrics_summary AS
SELECT 
    CASE 
        WHEN source_table = 'guangxi_news' THEN '广西'
        WHEN source_table = 'sichuan_news' THEN '四川'
        WHEN source_table = 'national_news' THEN '国家'
        ELSE source_table
    END as region,
    year,
    metric_key,
    metric_name,
    AVG(metric_value) as avg_value,
    MAX(metric_value) as max_value,
    MIN(metric_value) as min_value,
    COUNT(*) as sample_count
FROM health_ocr_metrics
WHERE metric_value IS NOT NULL
GROUP BY source_table, year, metric_key, metric_name;

-- ================================================================
-- 分析结果表 (供 Spark 处理任务写入)
-- ================================================================

-- 人口统计分析结果表 (按地区)
CREATE TABLE IF NOT EXISTS analysis_population_region (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100) NOT NULL COMMENT '地区名称',
    total_population BIGINT DEFAULT 0 COMMENT '总人口数',
    metric_type VARCHAR(50) DEFAULT 'by_region' COMMENT '指标类型',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='人口统计分析结果表(按地区)';

-- 人口统计分析结果表 (按年龄段)
CREATE TABLE IF NOT EXISTS analysis_population_age (
    id INT AUTO_INCREMENT PRIMARY KEY,
    age_group VARCHAR(50) NOT NULL COMMENT '年龄段',
    total_population BIGINT DEFAULT 0 COMMENT '总人口数',
    metric_type VARCHAR(50) DEFAULT 'by_age_group' COMMENT '指标类型',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_age_group (age_group)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='人口统计分析结果表(按年龄段)';

-- 人口统计分析结果表 (按性别)
CREATE TABLE IF NOT EXISTS analysis_population_gender (
    id INT AUTO_INCREMENT PRIMARY KEY,
    gender VARCHAR(20) NOT NULL COMMENT '性别',
    total_population BIGINT DEFAULT 0 COMMENT '总人口数',
    metric_type VARCHAR(50) DEFAULT 'by_gender' COMMENT '指标类型',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_gender (gender)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='人口统计分析结果表(按性别)';

-- 机构统计分析结果表 (按类型)
CREATE TABLE IF NOT EXISTS analysis_institution_type (
    id INT AUTO_INCREMENT PRIMARY KEY,
    type VARCHAR(100) NOT NULL COMMENT '机构类型',
    institution_count INT DEFAULT 0 COMMENT '机构数量',
    metric_type VARCHAR(50) DEFAULT 'by_type' COMMENT '指标类型',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_type (type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='机构统计分析结果表(按类型)';

-- 机构统计分析结果表 (按等级)
CREATE TABLE IF NOT EXISTS analysis_institution_level (
    id INT AUTO_INCREMENT PRIMARY KEY,
    level VARCHAR(50) NOT NULL COMMENT '机构等级',
    institution_count INT DEFAULT 0 COMMENT '机构数量',
    metric_type VARCHAR(50) DEFAULT 'by_level' COMMENT '指标类型',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_level (level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='机构统计分析结果表(按等级)';

-- 机构统计分析结果表 (按地区)
CREATE TABLE IF NOT EXISTS analysis_institution_region (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100) NOT NULL COMMENT '地区名称',
    institution_count INT DEFAULT 0 COMMENT '机构数量',
    metric_type VARCHAR(50) DEFAULT 'by_region' COMMENT '指标类型',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='机构统计分析结果表(按地区)';

-- 人员统计分析结果表
CREATE TABLE IF NOT EXISTS analysis_personnel (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100) NOT NULL COMMENT '地区名称',
    year INT NOT NULL COMMENT '统计年份',
    doctor_count INT DEFAULT 0 COMMENT '执业医师数',
    nurse_count INT DEFAULT 0 COMMENT '注册护士数',
    doctor_nurse_ratio DECIMAL(5,2) DEFAULT NULL COMMENT '医护比',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_region_year (region, year),
    INDEX idx_year (year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='人员统计分析结果表';

-- 床位统计分析结果表
CREATE TABLE IF NOT EXISTS analysis_beds (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100) NOT NULL COMMENT '地区名称',
    year INT NOT NULL COMMENT '统计年份',
    bed_count INT DEFAULT 0 COMMENT '实有床位数',
    avg_usage_rate DECIMAL(5,2) DEFAULT NULL COMMENT '平均病床使用率(%)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_region_year (region, year),
    INDEX idx_year (year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='床位统计分析结果表';

-- 服务统计分析结果表
CREATE TABLE IF NOT EXISTS analysis_services (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100) NOT NULL COMMENT '地区名称',
    year INT NOT NULL COMMENT '统计年份',
    outpatient_visits BIGINT DEFAULT 0 COMMENT '总诊疗人次数',
    discharge_count BIGINT DEFAULT 0 COMMENT '出院人数',
    avg_stay_days DECIMAL(5,2) DEFAULT NULL COMMENT '平均住院日',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_region_year (region, year),
    INDEX idx_year (year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='服务统计分析结果表';

-- 费用统计分析结果表
CREATE TABLE IF NOT EXISTS analysis_costs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100) NOT NULL COMMENT '地区名称',
    year INT NOT NULL COMMENT '统计年份',
    avg_outpatient_cost DECIMAL(10,2) DEFAULT NULL COMMENT '门诊次均费用(元)',
    avg_discharge_cost DECIMAL(10,2) DEFAULT NULL COMMENT '住院人均费用(元)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_region_year (region, year),
    INDEX idx_year (year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='费用统计分析结果表';

-- ================================================================
-- 分析功能表 (供分析功能模块使用)
-- ================================================================

-- OCR指标年度汇总表
CREATE TABLE IF NOT EXISTS ocr_metrics_yearly (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100) NOT NULL COMMENT '地区名称',
    year INT NOT NULL COMMENT '统计年份',
    doctor_count INT DEFAULT NULL COMMENT '执业(助理)医师数',
    nurse_count INT DEFAULT NULL COMMENT '注册护士数',
    bed_count INT DEFAULT NULL COMMENT '实有床位数',
    bed_usage_rate DECIMAL(5,2) DEFAULT NULL COMMENT '病床使用率(%)',
    outpatient_visits BIGINT DEFAULT NULL COMMENT '总诊疗人次数',
    discharge_count BIGINT DEFAULT NULL COMMENT '出院人数',
    avg_stay_days DECIMAL(5,2) DEFAULT NULL COMMENT '出院者平均住院日',
    outpatient_cost DECIMAL(10,2) DEFAULT NULL COMMENT '门诊病人次均医药费用(元)',
    discharge_cost DECIMAL(10,2) DEFAULT NULL COMMENT '出院病人人均医药费用(元)',
    data_source VARCHAR(50) DEFAULT 'ocr' COMMENT '数据来源',
    sample_count INT DEFAULT 1 COMMENT '样本数量',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_region_year (region, year),
    INDEX idx_year (year),
    INDEX idx_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='OCR指标年度汇总表';

-- 跨地区对比分析表
CREATE TABLE IF NOT EXISTS region_comparison (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100) NOT NULL COMMENT '地区名称',
    analysis_year INT NOT NULL COMMENT '分析年份',
    institution_count INT DEFAULT NULL COMMENT '医疗机构总数',
    top_hospital_count INT DEFAULT NULL COMMENT '三甲医院数量',
    doctors_per_10k DECIMAL(8,2) DEFAULT NULL COMMENT '每万人医师数',
    nurses_per_10k DECIMAL(8,2) DEFAULT NULL COMMENT '每万人护士数',
    beds_per_10k DECIMAL(8,2) DEFAULT NULL COMMENT '每万人床位数',
    avg_outpatient_per_doctor INT DEFAULT NULL COMMENT '医师日均诊疗人次',
    bed_turnover_rate DECIMAL(5,2) DEFAULT NULL COMMENT '床位周转次数',
    resource_score DECIMAL(5,2) DEFAULT NULL COMMENT '资源配置综合评分',
    service_score DECIMAL(5,2) DEFAULT NULL COMMENT '服务效率综合评分',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_region_year (region, analysis_year),
    INDEX idx_year (analysis_year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='跨地区对比分析表';

-- 预测结果表
CREATE TABLE IF NOT EXISTS prediction_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100) NOT NULL COMMENT '地区名称',
    metric_key VARCHAR(50) NOT NULL COMMENT '指标标识',
    metric_name VARCHAR(100) NOT NULL COMMENT '指标名称',
    predict_year INT NOT NULL COMMENT '预测年份',
    predict_value DECIMAL(15,2) NOT NULL COMMENT '预测值',
    confidence_lower DECIMAL(15,2) DEFAULT NULL COMMENT '置信区间下限',
    confidence_upper DECIMAL(15,2) DEFAULT NULL COMMENT '置信区间上限',
    model_type VARCHAR(50) DEFAULT 'linear_regression' COMMENT '模型类型',
    model_accuracy DECIMAL(5,4) DEFAULT NULL COMMENT '模型准确度',
    training_data_range VARCHAR(50) DEFAULT NULL COMMENT '训练数据范围',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_region_metric (region, metric_key),
    INDEX idx_year (predict_year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='预测结果表';

-- 异常检测结果表
CREATE TABLE IF NOT EXISTS anomaly_detection (
    id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100) NOT NULL COMMENT '地区名称',
    metric_key VARCHAR(50) NOT NULL COMMENT '指标标识',
    year INT NOT NULL COMMENT '数据年份',
    actual_value DECIMAL(15,2) NOT NULL COMMENT '实际值',
    expected_value DECIMAL(15,2) NOT NULL COMMENT '预期值',
    deviation_rate DECIMAL(8,4) NOT NULL COMMENT '偏离率(%)',
    anomaly_level ENUM('normal', 'warning', 'critical') DEFAULT 'normal' COMMENT '异常等级',
    description TEXT COMMENT '异常描述',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_region_year (region, year),
    INDEX idx_level (anomaly_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='异常检测结果表';

-- 完成提示
SELECT '✅ 六大模块分析视图和表创建完成！' as status;
