import mysql.connector

DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'rootpassword',
    'database': 'health_db',
}

TABLE_SQLS = [
    (
        'medical_institution',
        '''
        CREATE TABLE IF NOT EXISTS medical_institution (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            type VARCHAR(50),
            region VARCHAR(50),
            level VARCHAR(10),
            create_time DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
    ),
    (
        'population_info',
        '''
        CREATE TABLE IF NOT EXISTS population_info (
            id INT AUTO_INCREMENT PRIMARY KEY,
            region VARCHAR(50) NOT NULL,
            age_group VARCHAR(20),
            gender VARCHAR(10),
            population_count INT,
            create_time DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
    ),
    (
        'hospital_bed',
        '''
        CREATE TABLE IF NOT EXISTS hospital_bed (
            id INT AUTO_INCREMENT PRIMARY KEY,
            institution_id INT,
            total_count INT,
            occupied_count INT,
            CONSTRAINT fk_hospital_bed_institution
                FOREIGN KEY (institution_id) REFERENCES medical_institution(id)
                ON DELETE SET NULL ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
    ),
    (
        'population_data',
        '''
        CREATE TABLE IF NOT EXISTS population_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50),
            age INT,
            district VARCHAR(50),
            health_score INT,
            create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
            KEY idx_district (district)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
    ),
    (
        'guangxi_news',
        '''
        CREATE TABLE IF NOT EXISTS guangxi_news (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            link VARCHAR(512) NOT NULL,
            publish_date VARCHAR(50),
            source_category VARCHAR(100) DEFAULT '广西省卫生健康委员会',
            ocr_content LONGTEXT,
            detail_context LONGTEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_link (link(255))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
    ),
    (
        'national_news',
        '''
        CREATE TABLE IF NOT EXISTS national_news (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            link VARCHAR(512) NOT NULL,
            source_category VARCHAR(100),
            publish_date VARCHAR(50),
            ocr_content LONGTEXT,
            detail_context LONGTEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_link (link(255))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
    ),
    (
        'report_metrics',
        '''
        CREATE TABLE IF NOT EXISTS report_metrics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            report_id INT NOT NULL,
            metric_name VARCHAR(100) NOT NULL,
            metric_value VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_report_metric (report_id, metric_name),
            KEY idx_report_id (report_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
    ),
    (
        'health_ocr_metrics',
        '''
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
            context_json LONGTEXT,
            evidence_json LONGTEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_news_metric (news_id, metric_key),
            KEY idx_year_month (year, month),
            KEY idx_metric_key (metric_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
    ),
]


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        for name, sql in TABLE_SQLS:
            print(f'creating {name}...')
            cursor.execute(sql)
        conn.commit()
        print('schema repair done')
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
