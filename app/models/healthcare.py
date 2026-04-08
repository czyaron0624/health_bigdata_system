from app import db
from datetime import datetime

# 1. 人口信息表
class Population(db.Model):
    __tablename__ = 'population_info'
    id = db.Column(db.Integer, primary_key=True)
    region = db.Column(db.String(50), nullable=False)      # 地区
    age_group = db.Column(db.String(20))                   # 年龄段
    gender = db.Column(db.String(10))                      # 性别
    population_count = db.Column(db.Integer, default=0)    # 人口数量
    create_time = db.Column(db.DateTime, default=datetime.now)

# 2. 医疗机构表
class Institution(db.Model):
    __tablename__ = 'medical_institution'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)       # 机构名称
    type = db.Column(db.String(50))                        # 类型(综合医院/社区中心等)
    region = db.Column(db.String(50))                      # 地区
    level = db.Column(db.String(10))                       # 等级(如：三甲)
    create_time = db.Column(db.DateTime, default=datetime.now)

# 3. 医疗床位表
class HospitalBed(db.Model):
    __tablename__ = 'hospital_bed'
    id = db.Column(db.Integer, primary_key=True)
    institution_id = db.Column(db.Integer, db.ForeignKey('medical_institution.id'))
    total_count = db.Column(db.Integer, default=0)         # 总床位数
    occupied_count = db.Column(db.Integer, default=0)      # 在用床位数