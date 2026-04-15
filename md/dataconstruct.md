1. 建立数据整理层视图（create_vw_metric_clean.sql）
我新建了一个数据库视图 vw_metric_clean。

它汇聚了 health_ocr_metrics 原始表所有需要用到的字段。
它已经自动执行了数据有效性清理：去除了 year 或 metric_value 为空的数据，并且自带原先散落在后端代码里的指标范围过滤（比如 doctor_count BETWEEN 1000 AND 300000 ）。
好处：这层成了数据的只读“契约版本”。即使后面换列式数据库，后端代码也不用变。
2. 后端聚合与概览接口收敛（app.py）

/api/analysis/structured-report-summary（趋势）和 /api/analysis/data-summary（概览）等接口现在改为只查 vw_metric_clean 视图。
清理了长篇大论的写死在 Python 里的聚合代码常数 OCR_METRIC_VALID_SQL。
好处：保证了所有宏观图表、趋势展现的一致性——脏数据被彻底拒之门外，图表永远不抖动。
3. 新增最小数据明细接口（app.py:862-913）

新增了一个 /api/analysis/metric-details 接口给前端表格直连或导出按钮用。
支持按 scope (来源省份)、metric_key、year 的组合查询，并内置了标准分页 (page / page_size) 与对应的 total 下发。
返回的每一行都会带着 news_id 与 source_table 给前端，用以溯源链接。
你可以直接联调验收

启动你的 Web 容器或 Flask App。
调用 GET /api/analysis/metric-details?scope=sichuan&metric_key=doctor_count&year=2021&page=1&page_size=5。
把返回的 JSON 丢给前端。这套接口里，所有字段类型和空置约定都是稳定的。未来新增再多省份，甚至后续 Spark 介入进来出汇总库，也只需要维护底层的视图或换数据表，上面接口永远不用动都不用动！