from app import create_app

# 实例化我们的 Flask 应用
app = create_app()

if __name__ == '__main__':
    # 启动 Web 服务
    app.run(host='0.0.0.0', port=5000, debug=True)