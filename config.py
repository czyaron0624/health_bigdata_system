import os

class Config:
    # 基础配置
    SECRET_KEY = 'health-system-secret-key'
    
    # MySQL 数据库连接配置 (连接到 Docker)
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:rootpassword@localhost:3306/health_db'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Redis 缓存配置 (连接到 Docker)
    REDIS_URL = "redis://localhost:6379/0"