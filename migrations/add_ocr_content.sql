-- 为 guangxi_news 表添加 OCR 内容字段
-- 执行日期: 2026-04-10

-- 检查字段是否已存在，如果不存在则添加
ALTER TABLE guangxi_news 
ADD COLUMN IF NOT EXISTS ocr_content TEXT COMMENT 'OCR识别的文字内容';

-- 添加索引以提高搜索性能（可选）
-- ALTER TABLE guangxi_news ADD INDEX idx_ocr_content (ocr_content(100));

-- 验证表结构
DESCRIBE guangxi_news;