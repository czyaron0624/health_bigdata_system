from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate  # 引入迁移工具
from redis import Redis
from config import Config

# 初始化插件
db = SQLAlchemy()
migrate = Migrate()

def create_app():
    app = Flask(__name__, template_folder='../templates') # 明确告诉它模板在哪
    app.config.from_object(Config)

    db.init_app(app)
    migrate.init_app(app, db)
    app.redis = Redis.from_url(app.config['REDIS_URL'])

    with app.app_context():
        from app.models import healthcare
        
        # --- 新增：注册首页路由 ---
        from app.routes.home import home_bp
        app.register_blueprint(home_bp)
        # -----------------------

    return app