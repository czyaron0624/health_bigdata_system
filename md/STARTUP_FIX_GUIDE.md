# 🔧 应用启动失败 - 问题诊断和解决方案

## 问题描述
启动应用后，cmd窗口直接关闭，导致网页出现 `127.0.0.1` 连接被拒绝错误。

## 根本原因 ✓
应用导入失败，有两个关键问题：

### 问题1️⃣：缺失依赖 - `mysql-connector-python`
- `document.py` 使用 `import mysql.connector`
- 但 `requirements.txt` 中没有 `mysql-connector-python` 包
- 仅有 `PyMySQL` 包，导致导入失败

### 问题2️⃣：Python模块导入路径不正确
- 在 `app.py` 中 `from document import document_bp` 可能报错
- 跨模块导入时没有正确的fallback机制

### 问题3️⃣：document.py 内部导入
- `from document_parser import ...` 相对导入在某些情况下失败

## 解决方案 ✅

### 修改1：使用已有的 PyMySQL 替代 mysql-connector
**文件**: `web_app/document.py` 第6行

```python
# ❌ 旧版本
import mysql.connector

# ✅ 新版本
import pymysql
```

并修改数据库连接方式：
```python
# ❌ 旧版本
conn = mysql.connector.connect(host='localhost', port=3307, ...)

# ✅ 新版本
conn = pymysql.connect(host='localhost', port=3307, charset='utf8mb4')
```

### 修改2：添加导入fallback机制
**文件**: `web_app/document.py` 顶部

```python
try:
    from .document_parser import parse_document, extract_healthcare_data
except ImportError:
    # Fallback方案
    import importlib.util
    spec = importlib.util.spec_from_file_location("document_parser", ...)
    ...
```

### 修改3：改进蓝图导入
**文件**: `web_app/app.py` 第850-853行

```python
# ❌ 旧版本
from document import document_bp

# ✅ 新版本
try:
    from .document import document_bp
except ImportError:
    from document import document_bp
```

## 新增启动脚本

### 1️⃣ `start_debug.bat` - 调试启动
显示详细错误信息而不是直接关闭窗口

```batch
用法: 双击运行 start_debug.bat
特点: 任何错误都会显示详细信息，窗口不会自动关闭
```

### 2️⃣ `start_simple.ps1` - 简化启动（推荐）
智能检查Docker和依赖，提供友好的启动提示

```powershell
用法: .\start_simple.ps1
特点: 
  • 检查虚拟环境
  • 自动尝试启动Docker容器
  • 显示访问地址和凭证
  • 清晰的错误信息
```

## 验证修复 ✅

```bash
# 验证Flask应用能否正确导入
python -c "from web_app.app import app; print('✅ Flask应用导入成功')"

# 验证没有语法错误
python -m py_compile web_app/*.py
```

## 测试访问

修复后，您可以通过以下方式启动应用：

### 方式1️⃣：使用新的启动脚本（推荐）
```powershell
# 在PowerShell中运行
.\start_simple.ps1

# 或者在cmd中运行
powershell -ExecutionPolicy Bypass -File start_simple.ps1
```

### 方式2️⃣：调试模式启动
```batch
# 双击运行
start_debug.bat
```

### 方式3️⃣：直接启动
```bash
# 激活虚拟环境
.\venv\Scripts\Activate.ps1

# 运行应用
python run.py
```

## 访问应用

启动后，打开浏览器访问：

| 功能 | URL | 凭证 |
|-----|-----|------|
| 登录 | http://127.0.0.1:5000/login | admin/admin123 |
| 管理员仪表板 | http://127.0.0.1:5000/admin/dashboard | - |
| 用户仪表板 | http://127.0.0.1:5000/user/dashboard | user/user123 |
| 文档上传 | 在管理员仪表板中的"📤文档上传"按钮 | - |

## 常见问题

### Q: 启动后还是显示 127.0.0.1 拒绝连接？
A: 尝试以下步骤：
1. 确保 Docker 已启动
2. 确保 MySQL (3307) 和 Redis (6379) 正在运行
3. 检查是否有其他进程占用 5000 端口
4. 在 `start_debug.bat` 中查看具体错误

### Q: 双击脚本没有反应？
A: 使用 PowerShell 运行：
```powershell
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope CurrentUser
.\start_simple.ps1
```

### Q: 数据库连接失败？
A: 确保 MySQL 配置正确：
```
主机: localhost
端口: 3307
用户: root
密码: rootpassword
数据库: health_db
```

## 文件修改清单

| 文件 | 修改内容 | 影响 |
|-----|--------|------|
| `web_app/document.py` | 导入改为 pymysql，添加fallback机制 | ✅ 修复导入错误 |
| `web_app/app.py` | 添加蓝图导入fallback | ✅ 改进鲁棒性 |
| `start_debug.bat` | 新增 | ℹ️ 调试工具 |
| `start_simple.ps1` | 新增 | ℹ️ 便利工具 |

---

**问题解决状态**: ✅ 已完全解决  
**测试状态**: ✅ Flask应用导入成功  
**推荐启动方式**: `.\start_simple.ps1`
