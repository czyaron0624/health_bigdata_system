from flask import Blueprint, render_template
from app.models.healthcare import Institution, Population

# 创建一个名为 'home' 的蓝图
home_bp = Blueprint('home', __name__)

@home_bp.route('/')
def index():
    # 查询一下数据库，看看有没有数据（虽然现在是0）
    inst_count = Institution.query.count()
    pop_count = Population.query.count()
    
    # 渲染 templates 目录下的 index.html
    return render_template('index.html', inst_count=inst_count, pop_count=pop_count)