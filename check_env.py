from app import create_app, db
from sqlalchemy import text

# 创建 Flask 应用实例，加载配置
app = create_app()

def run_health_check():
    with app.app_context():
        print("\n" + "="*40)
        print("🚀 开始进行基础环境体检...")
        print("="*40)
        
        # 1. 测试 MySQL 连通性
        print("\n[1/2] 正在测试 MySQL 数据库连接...")
        try:
            # 执行最简单的 SQL 语句
            db.session.execute(text('SELECT 1'))
            print("   ✅ MySQL 连接成功！Python 已成功连入 Docker 中的 MySQL。")
        except Exception as e:
            print(f"   ❌ MySQL 连接失败！错误信息：\n   {e}")
            
        # 2. 测试 Redis 连通性
        print("\n[2/2] 正在测试 Redis 缓存连接...")
        try:
            # 向 Redis 发送 ping 命令
            app.redis.ping()
            print("   ✅ Redis 连接成功！Python 已成功连入 Docker 中的 Redis。")
        except Exception as e:
            print(f"   ❌ Redis 连接失败！错误信息：\n   {e}")
            
        print("\n" + "="*40)
        print("🏁 体检结束！")
        print("="*40 + "\n")

if __name__ == '__main__':
    run_health_check()