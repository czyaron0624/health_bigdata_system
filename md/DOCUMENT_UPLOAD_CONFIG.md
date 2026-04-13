# 文档上传功能配置指南

> **最新更新**：上传功能已集成到管理员仪表板，采用现代化模态框界面

## ✅ 解决的问题

- ✅ 修复导入路径问题（services 文件夹不存在）
- ✅ 修复相对导入和配置访问
- ✅ 完善 Flask 应用配置
- ✅ **将独立页面转换为管理员仪表板模态框**（解决冲突）

## 🎨 前端集成方案

### 按钮位置
"管理快捷操作"卡片中新增"📤 文档上传"按钮

### 模态框特性
- 现代化界面（与仪表板风格统一）
- 支持拖拽和点击上传
- 实时预览解析结果（表格形式）
- 自动识别数据表类型
- 进度显示与错误提示
- 成功后自动关闭

## 🔧 API 端点

### `POST /upload/preview`
上传并预览文件

**请求**: multipart/form-data, file  
**响应**: `{success, target_table, data[]}`

### `POST /upload/confirm`
导入数据到数据库

**请求**: JSON, {target_table, data[]}  
**响应**: `{success, inserted_count}`

## 📁 支持格式

Excel (.xlsx, .xls) | PDF | Word (.docx) | Text (.txt)

## 🧠 数据识别

- **人口** → population_info
- **机构/医院** → medical_institution  
- **其他** → health_ocr_metrics

## 🚀 使用流程

1. 访问 `/login` → 管理员账号登录
2. 进入 `/admin/dashboard` 仪表板
3. 找到"📤 文档上传"按钮
4. 拖拽或点击选择文件
5. 预览确认后点击"确认入库"
6. 完成！

## ⚙️ 配置

- **上传限制**: 100MB
- **上传文件夹**: web_app/uploads/
- **数据库**: localhost:3307, health_db

## 📊 文件修改

- ✅ admin_dashboard.html - 添加模态框 UI（约400行新代码）
- ✅ document.py - 改为返回信息提示
- ✅ app.py - 配置和蓝图注册
- ✅ document_parser.py - 导入路径修复

---

**版本**: v1.1 | **状态**: ✅ 已测试 | **日期**: 2026-04-13
