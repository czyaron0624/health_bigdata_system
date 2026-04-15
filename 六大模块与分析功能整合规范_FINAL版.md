# 六大模块与分析功能整合规范（FINAL 版）

版本: v2.0-final  
创建日期: 2026-04-15  
适用项目: 医疗健康大数据系统  
文档定位: 本文档为六大模块与分析功能的一体化交付规范，作为数据库、Spark、后端 API、前端联调和验收的统一依据。  
说明: 本文件为新增最终版文档，不替换原有文档。

---

## 1. 编写目标

本版文档用于解决原规范中的三类问题：

1. 统一指标口径，避免同一指标在不同模块中含义不一致。
2. 明确当前实现状态，避免“文档已写但代码未落地”的误判。
3. 补齐实施、验收、数据质量和启动步骤，使规范可直接指导开发与联调。

---

## 2. 适用范围

本文档覆盖以下内容：

1. 六大基础分析模块
2. 历史趋势、跨地区对比、预测分析、异常预警四类分析能力
3. MySQL 原始表、分析结果表、数据库视图
4. Spark 聚合任务
5. Flask API 接口
6. 联调、验收与运行规范

不包含以下内容：

1. 页面视觉设计细节
2. 用户权限和登录流程细节
3. OCR 抽取算法实现细节

---

## 3. 系统现状与目标态

### 3.1 当前已存在内容

仓库中已存在以下核心文件：

- `migrations/create_analysis_views.sql`
- `migrations/add_health_ocr_metrics.sql`
- `spark_job/six_modules_processor.py`
- `web_app/analysis_api.py`
- `web_app/app.py`

当前已具备：

1. 六大模块基础表和部分分析结果表设计
2. 六大模块 Spark 聚合脚本
3. 六大模块与分析能力的基础 API 路由
4. 年度指标、区域对比、预测结果、异常预警的基础接口占位

### 3.2 当前未完整落地内容

以下能力在文档中有定义，但仓库内尚未形成完整独立实现文件：

- `spark_job/trend_prediction.py`
- `spark_job/anomaly_detection.py`

因此本规范中，相关内容统一标记为：

- `已实现`：仓库中已有可运行文件或明确 SQL/API 实现
- `部分实现`：存在接口或表结构，但返回字段、口径或处理链路未完整闭环
- `待实现`：仅有目标设计，未形成实际文件或完整流程

### 3.3 文档使用原则

从本版开始，所有研发活动遵循以下规则：

1. 文档中的“当前实现”以仓库现状为准。
2. 文档中的“目标规范”用于后续补齐。
3. 前后端联调时，以“接口状态表”判断是否可直接对接。
4. 不允许前端、后端、数据侧分别维护不同指标口径。

---

## 4. 总体架构

系统采用四层结构：

1. 原始数据层
2. 规范视图层
3. 分析结果层
4. 服务展示层

数据主链路如下：

`原始表 -> 规范视图/清洗规则 -> Spark 聚合或计算 -> 分析结果表 -> Flask API -> 前端展示`

### 4.1 原始数据层

主要表如下：

- `population_info`
- `medical_institution`
- `hospital_bed`
- `health_ocr_metrics`
- `guangxi_news`

### 4.2 规范视图层

主要视图如下：

- `v_population_stats`
- `v_institution_stats`
- `v_personnel_stats`
- `v_bed_stats`
- `v_service_stats`
- `v_cost_stats`
- `v_health_metrics_summary`

视图层的职责：

1. 统一字段含义
2. 屏蔽原始表差异
3. 为 API 与 Spark 提供稳定输入

### 4.3 分析结果层

分析结果表分为两类：

1. 六大模块结果表
2. 高阶分析结果表

六大模块结果表：

- `analysis_population_region`
- `analysis_population_age`
- `analysis_institution_type`
- `analysis_institution_level`
- `analysis_institution_region`
- `analysis_personnel`
- `analysis_beds`
- `analysis_services`
- `analysis_costs`

高阶分析结果表：

- `ocr_metrics_yearly`
- `region_comparison`
- `prediction_results`
- `anomaly_detection`

### 4.4 服务展示层

服务层由 Flask 提供 REST API，对外暴露：

- 六大模块统计接口
- 历史汇总接口
- 区域对比接口
- 预测结果接口
- 异常预警接口

---

## 5. 数据口径统一规范

本章为本次 Final 版最重要部分。

### 5.1 地区字段统一规范

#### 5.1.1 标准字段定义

统一使用以下概念：

- `region`：标准化后的地区名称，用于所有分析结果表与 API 返回
- `source_table`：原始来源表标识，仅用于追溯数据来源，不直接作为业务地区

#### 5.1.2 当前现状

`health_ocr_metrics` 当前表结构中存在 `source_table`，但没有 `region` 字段。  
因此，凡是基于 `health_ocr_metrics` 计算地区维度的模块，必须通过映射规则生成标准化 `region`。

#### 5.1.3 当前映射规则

默认映射如下：

- `guangxi_news -> 广西`
- `sichuan_news -> 四川`
- `national_news -> 国家`
- 其他值 -> 保留原值，但视为待治理数据

#### 5.1.4 约束要求

1. API 对外只返回标准化后的 `region`。
2. Spark 聚合必须在写入结果表前完成地区标准化。
3. 后续如新增来源表，必须同步更新地区映射表或映射规则。

### 5.2 年份字段统一规范

统一使用 `year` 表示统计年份，要求：

1. 类型为整数
2. 空值不得参与趋势分析和预测训练
3. 预测结果中的年份字段固定命名为 `predict_year`
4. 区域对比中的分析基准年份固定命名为 `analysis_year`

### 5.3 指标单位统一规范

统一要求如下：

- 比例类指标使用百分比，字段值直接存数值，不带 `%`
- 人均类指标必须在字段名中体现分母单位
- 金额类指标单位固定为人民币元
- 趋势图中不可混用“每千人”和“每万人”

推荐统一口径：

- `doctors_per_1000`
- `nurses_per_1000`
- `beds_per_1000`

若跨地区对比保留 `per_10k` 口径，则必须在文档和接口中单独说明，不得与 `per_1000` 混用展示。

### 5.4 空值与异常值处理规范

1. 空值不参与平均值计算
2. 缺失关键字段的记录不得直接进入分析结果表
3. `bed_usage_rate` 合法区间为 `0-100`
4. 金额字段不得为负数
5. 人数和次数类指标不得为负数

---

## 6. 六大基础模块规范

以下六个模块均分为：数据来源、核心指标、计算口径、结果表、接口状态。

### 6.1 人口信息统计分析

状态: `已实现`

数据来源：

- `population_info`

核心指标：

- 总人口数
- 按地区人口分布
- 按年龄段人口分布
- 男女比例

计算口径：

1. 总人口数 = `SUM(population_count)`
2. 地区人口 = 按 `region` 聚合
3. 年龄段人口 = 按 `age_group` 聚合
4. 男女比例 = 男性人口 / 女性人口

结果表：

- `analysis_population_region`
- `analysis_population_age`

当前接口：

- `GET /api/analysis/population`

规范返回应包含：

- `total_population`
- `by_region`
- `by_age_group`
- `gender_ratio`

说明：

当前代码接口已具备基础区域统计，但年龄段和性别口径仍建议统一补齐。

### 6.2 医疗卫生机构统计分析

状态: `已实现`

数据来源：

- `medical_institution`

核心指标：

- 机构总数
- 按类型分布
- 按等级分布
- 按地区分布

计算口径：

1. 机构总数 = `COUNT(*)`
2. 类型分布 = 按 `type` 聚合
3. 等级分布 = 按 `level` 聚合
4. 地区分布 = 按 `region` 聚合

结果表：

- `analysis_institution_type`
- `analysis_institution_level`
- `analysis_institution_region`

当前接口：

- `GET /api/analysis/institutions`

说明：

当前接口已能返回类型、等级和地区分布，但尚未严格按参数条件组合过滤，后续需要补齐筛选逻辑。

### 6.3 医疗卫生人员统计分析

状态: `部分实现`

数据来源：

- `health_ocr_metrics`

使用指标：

- `doctor_count`
- `nurse_count`

核心指标：

- 执业医师总数
- 注册护士总数
- 医护比
- 各地区人员分布
- 每千人医师数

计算口径：

1. 医师总数 = `SUM(metric_value where metric_key='doctor_count')`
2. 护士总数 = `SUM(metric_value where metric_key='nurse_count')`
3. 医护比 = 医师数 / 护士数
4. 每千人医师数 = 医师数 / 总人口 * 1000

结果表：

- `analysis_personnel`

当前接口：

- `GET /api/analysis/personnel`

当前问题：

1. 现有实现中地区来源仍带有 `source_table` 痕迹
2. 文档目标中定义了 `doctors_per_1000`，当前接口未稳定返回
3. 年度趋势与当期汇总尚未完全拆分

### 6.4 医疗卫生床位统计分析

状态: `部分实现`

数据来源：

- `health_ocr_metrics`
- `hospital_bed`

使用指标：

- `bed_count`
- `bed_usage_rate`

核心指标：

- 实有床位总数
- 平均病床使用率
- 每千人床位数
- 各地区床位分布
- 年度趋势

结果表：

- `analysis_beds`

当前接口：

- `GET /api/analysis/beds`

当前问题：

1. 文档中写明联合 `hospital_bed`，但当前实现主要依赖 `health_ocr_metrics`
2. `beds_per_1000` 需要依赖人口基数，当前链路未完全闭环

### 6.5 医疗服务统计分析

状态: `部分实现`

数据来源：

- `health_ocr_metrics`

使用指标：

- `outpatient_visits`
- `discharge_count`
- `avg_stay_days`

核心指标：

- 总诊疗人次数
- 出院人数
- 平均住院日
- 医师日均诊疗人次
- 各地区服务量分布

结果表：

- `analysis_services`

当前接口：

- `GET /api/analysis/services`

当前问题：

1. `outpatient_per_doctor_per_day` 需要结合人员统计与时间口径
2. 当前接口更偏基础聚合，未完全达到规范目标

### 6.6 医疗费用统计分析

状态: `部分实现`

数据来源：

- `health_ocr_metrics`

使用指标：

- `outpatient_cost`
- `discharge_cost`

核心指标：

- 门诊次均费用
- 住院人均费用
- 年度趋势
- 各地区费用对比
- 增长率

结果表：

- `analysis_costs`

当前接口：

- `GET /api/analysis/costs`

当前问题：

1. 当前接口已具备基础费用输出
2. 增长率与趋势指标需单独定义计算逻辑

---

## 7. 分析功能模块规范

### 7.1 历史趋势分析

状态: `部分实现`

接口：

- `GET /api/metrics/yearly`

数据来源：

- `ocr_metrics_yearly`

目标能力：

1. 返回按地区、年份组织的年度汇总指标
2. 支持 `scope`、`region`、`year` 过滤
3. 为前端趋势图直接提供结构化数据

最低返回字段：

- `region`
- `year`
- `doctor_count`
- `nurse_count`
- `bed_count`
- `outpatient_visits`
- `discharge_count`
- `meta.total`

说明：

当前接口已存在，但筛选维度与字段完整性仍需继续补齐。

### 7.2 跨地区对比分析

状态: `部分实现`

接口：

- `GET /api/analysis/region-comparison`

数据来源：

- `region_comparison`

核心指标建议统一为以下一套：

- `institution_count`
- `top_hospital_count`
- `doctors_per_10k`
- `nurses_per_10k`
- `beds_per_10k`
- `resource_score`
- `service_score`

说明：

若系统最终决定全部改用每千人口径，则需要同步修改表结构、接口字段和前端图表，不得只改文档。

### 7.3 预测分析

状态: `待实现`

接口：

- `GET /api/prediction/results`

数据来源：

- `prediction_results`

模型要求：

1. 最少连续 3 年有效历史数据方可训练
2. 默认预测未来 3 年
3. 必须记录 `model_type`
4. 必须记录 `model_accuracy`
5. 必须保留预测区间上下界

推荐首版实现：

1. 先使用线性回归
2. 加入训练区间记录
3. 支持按 `metric_key` 和 `region` 查询

### 7.4 异常预警

状态: `待实现`

接口：

- `GET /api/anomaly/alerts`

数据来源：

- `anomaly_detection`

首版规则：

1. 偏离率绝对值 `> 20%` 且 `< 50%` 记为 `warning`
2. 偏离率绝对值 `>= 50%` 记为 `critical`

建议补充：

1. 支持按地区、年份、指标筛选
2. 增加是否已处理字段
3. 增加告警生成时间和处理时间

---

## 8. 数据库设计规范

### 8.1 设计原则

1. 原始数据表只做采集与沉淀
2. 视图负责口径统一
3. 结果表负责服务查询
4. 高阶分析表负责趋势、预测、预警输出

### 8.2 API 读取规则

为避免两套口径并存，统一规定：

1. 六大模块 API 默认读取 `analysis_*` 结果表
2. 历史趋势 API 读取 `ocr_metrics_yearly`
3. 跨地区对比 API 读取 `region_comparison`
4. 预测 API 读取 `prediction_results`
5. 异常预警 API 读取 `anomaly_detection`

视图主要用于：

1. Spark 输入
2. 数据核对
3. 临时分析

不建议前端接口直接长期读取视图。

### 8.3 主键与唯一键要求

所有结果表必须具备稳定唯一约束。

建议约束如下：

- `analysis_personnel(region, year)`
- `analysis_beds(region, year)`
- `analysis_services(region, year)`
- `analysis_costs(region, year)`
- `ocr_metrics_yearly(region, year)`
- `region_comparison(region, analysis_year)`

### 8.4 字段命名要求

统一要求：

1. 表名使用小写下划线
2. 指标字段名使用英文业务语义
3. 同类字段命名保持一致
4. 不允许同一概念在不同表中使用多个命名

例如：

- 使用 `discharge_count`，不要同时出现 `total_discharge`
- 使用 `avg_usage_rate`，不要同时出现 `bed_usage_avg`

---

## 9. API 规范

### 9.1 通用要求

所有接口默认：

- 方法: `GET`
- 返回格式: `application/json`
- 编码: `UTF-8`

### 9.2 成功响应格式

```json
{
  "success": true,
  "data": {},
  "meta": {
    "total": 0,
    "page": 1,
    "page_size": 20
  }
}
```

说明：

1. `meta` 在分页或统计列表场景中必须返回
2. 非分页场景可只返回 `meta.total`

### 9.3 错误响应格式

```json
{
  "success": false,
  "error": "错误描述信息",
  "error_code": "INVALID_PARAMETER"
}
```

### 9.4 参数校验要求

1. `year` 必须为正整数
2. `limit` 必须为 `1-100`
3. `scope` 只允许已定义范围
4. `level` 只允许 `warning` 或 `critical`

### 9.5 接口状态表

| 接口 | 目标状态 | 当前状态 | 备注 |
|---|---|---|---|
| `/api/analysis/population` | 完整可用 | 已实现 | 建议补年龄段与性别口径 |
| `/api/analysis/institutions` | 完整可用 | 已实现 | 建议补组合筛选 |
| `/api/analysis/personnel` | 完整可用 | 部分实现 | 地区与人均口径需统一 |
| `/api/analysis/beds` | 完整可用 | 部分实现 | 人均床位需接入人口分母 |
| `/api/analysis/services` | 完整可用 | 部分实现 | 服务效率指标待补 |
| `/api/analysis/costs` | 完整可用 | 部分实现 | 增长率待补 |
| `/api/metrics/yearly` | 完整可用 | 部分实现 | 字段完整性待补 |
| `/api/analysis/region-comparison` | 完整可用 | 部分实现 | 综合评分口径需固定 |
| `/api/prediction/results` | 可查询预测结果 | 部分实现 | 依赖预测任务补齐 |
| `/api/anomaly/alerts` | 可查询告警结果 | 部分实现 | 依赖异常检测任务补齐 |

---

## 10. Spark 任务规范

### 10.1 六大模块聚合任务

文件：

- `spark_job/six_modules_processor.py`

状态: `已实现`

职责：

1. 读取原始表
2. 完成六大模块聚合
3. 写入 `analysis_*` 结果表

要求：

1. 聚合前先标准化 `region`
2. 写表前校验关键字段非空
3. 失败时输出明确日志

### 10.2 趋势预测任务

目标文件：

- `spark_job/trend_prediction.py`

状态: `待实现`

职责：

1. 从年度汇总表读取训练数据
2. 按地区和指标建模
3. 写入 `prediction_results`

### 10.3 异常检测任务

目标文件：

- `spark_job/anomaly_detection.py`

状态: `待实现`

职责：

1. 读取实际值与预期值
2. 计算偏离率
3. 写入 `anomaly_detection`

---

## 11. 数据质量与治理要求

### 11.1 必检项

每次跑批后必须至少检查：

1. 地区字段是否已标准化
2. 年份字段是否为空
3. 关键指标是否为负值
4. 比例字段是否超范围
5. 唯一键是否冲突

### 11.2 治理分级

分为三类问题：

1. `阻断级`：关键字段为空、主键冲突、比例非法
2. `警告级`：样本量异常、地区名称不规范、增长率异常
3. `提示级`：备注缺失、来源未映射

### 11.3 质量门禁

满足以下条件后方可进入联调：

1. 六大模块结果表全部生成成功
2. 核心接口成功率达到 100%
3. 抽样核对至少 10 条记录无明显口径错误
4. 没有阻断级数据问题

---

## 12. 实施计划与验收标准

### 12.1 第一阶段：六大模块

目标：

1. 六大基础结果表可稳定生成
2. 六大基础接口可供前端调用

验收标准：

1. Spark 跑批完成且无异常终止
2. 六大模块 API 全部可返回有效 JSON
3. 返回字段名与文档一致

### 12.2 第二阶段：历史汇总与区域对比

目标：

1. 年度汇总表可稳定查询
2. 区域对比接口可输出统一口径指标

验收标准：

1. `/api/metrics/yearly` 可按年份和范围过滤
2. `/api/analysis/region-comparison` 输出字段完整

### 12.3 第三阶段：预测分析

目标：

1. 预测任务可生成未来 3 年结果
2. 预测接口可查询预测值和置信区间

验收标准：

1. `prediction_results` 有可用样例数据
2. 接口返回包含模型类型和准确度

### 12.4 第四阶段：异常预警

目标：

1. 能自动识别异常指标
2. 能按等级查询告警结果

验收标准：

1. `anomaly_detection` 有有效告警样例
2. `/api/anomaly/alerts` 支持 `level` 和 `limit`

---

## 13. 文件与职责对应表

| 文件 | 职责 | 当前状态 |
|---|---|---|
| `migrations/create_analysis_views.sql` | 创建视图与分析结果表 | 已实现 |
| `migrations/add_health_ocr_metrics.sql` | 创建 OCR 指标表 | 已实现 |
| `spark_job/six_modules_processor.py` | 六大模块聚合处理 | 已实现 |
| `web_app/analysis_api.py` | 分析 API 路由与查询 | 已实现 |
| `web_app/app.py` | Flask 主应用入口 | 已实现 |
| `spark_job/trend_prediction.py` | 趋势预测任务 | 待实现 |
| `spark_job/anomaly_detection.py` | 异常检测任务 | 待实现 |

---

## 14. 推荐启动顺序

### 14.1 数据准备

1. 创建数据库基础表
2. 执行 OCR 指标表脚本
3. 导入或准备测试数据

### 14.2 创建视图与结果表

```bash
mysql -u root -p health_db < migrations/create_analysis_views.sql
```

### 14.3 执行六大模块聚合

```bash
python spark_job/six_modules_processor.py
```

### 14.4 启动后端服务

```bash
python web_app/app.py
```

### 14.5 基础接口验证

```bash
curl http://localhost:5000/api/analysis/population
curl http://localhost:5000/api/analysis/institutions
curl http://localhost:5000/api/analysis/personnel
curl http://localhost:5000/api/analysis/beds
curl http://localhost:5000/api/analysis/services
curl http://localhost:5000/api/analysis/costs
curl http://localhost:5000/api/metrics/yearly
curl http://localhost:5000/api/analysis/region-comparison
curl http://localhost:5000/api/prediction/results
curl http://localhost:5000/api/anomaly/alerts
```

---

## 15. 后续优化建议

建议下一步按以下顺序推进：

1. 把 `health_ocr_metrics` 的地区口径彻底标准化
2. 让六大模块 API 全部改为查询 `analysis_*` 结果表
3. 补齐趋势预测与异常检测任务
4. 增加自动化校验脚本和接口测试
5. 增加“实现状态”维护区，避免文档与代码再度脱节

---

## 16. 结论

本 Final 版文档相较原始版本，重点完成了以下增强：

1. 增加“当前实现”和“目标规范”的边界
2. 补齐地区口径、指标单位、接口状态和验收标准
3. 明确 API、视图、结果表之间的职责分层
4. 为后续开发提供了更稳定的基线

本文件可作为当前阶段的统一执行规范使用。
