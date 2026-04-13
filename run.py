from web_app.app import app

if __name__ == '__main__':
    # 启动完整 Web 应用（包含 /login 与仪表盘相关路由）
    app.run(host='0.0.0.0', port=5000, debug=True)