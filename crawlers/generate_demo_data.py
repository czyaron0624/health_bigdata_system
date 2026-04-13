"""
为系统生成演示统计数据
模拟从公报中提取的真实健康指标
"""

import mysql.connector
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DemoDataGenerator:
    def __init__(self):
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "rootpassword",
            "database": "health_db"
        }

    def connect_db(self):
        try:
            conn = mysql.connector.connect(**self.db_config)
            return conn
        except mysql.connector.Error as err:
            logger.error(f"数据库连接失败: {err}")
            raise

    def generate_demo_institutions(self):
        """生成演示的医疗卫生机构数据"""
        
        logger.info("=" * 60)
        logger.info("正在生成医疗机构演示数据...")
        logger.info("=" * 60)
        
        # 演示医疗机构数据
        demo_institutions = [
            # 综合医院
            ("广西医科大学第一附属医院", "综合医院", "广西", "三甲"),
            ("广西医科大学第二附属医院", "综合医院", "广西", "三甲"),
            ("广西人民医院", "综合医院", "广西", "三甲"),
            ("南宁市第一人民医院", "综合医院", "南宁", "三级"),
            ("南宁市第二人民医院", "综合医院", "南宁", "三级"),
            ("柳州市人民医院", "综合医院", "柳州", "三级"),
            ("桂林医学院附属医院", "综合医院", "桂林", "三级"),
            ("梧州市红十字会医院", "综合医院", "梧州", "二级"),
            ("北海市人民医院", "综合医院", "北海", "二级"),
            ("钦州市第一人民医院", "综合医院", "钦州", "二级"),
            
            # 专科医院
            ("广西肿瘤医院", "肿瘤专科", "广西", "三级"),
            ("广西精神卫生中心", "精神专科", "广西", "三级"),
            ("广西妇幼保健院", "妇幼专科", "广西", "三级"),
            ("广西传染病防治研究所", "传染病防治", "广西", "二级"),
            ("南宁市妇幼保健院", "妇幼专科", "南宁", "二级"),
            
            # 卫生院/卫生所
            ("南宁青秀区卫生院", "卫生院", "南宁", "一级"),
            ("南宁江南区中医医院", "中医医院", "南宁", "一级"),
            ("柳州鱼峰区卫生院", "卫生院", "柳州", "一级"),
            ("桂林象山区中医诊所", "卫生所", "桂林", "一级"),
            ("梧州岑溪县妇幼卫生院", "卫生院", "梧州", "一级"),
            ("北海合浦县卫生院", "卫生院", "北海", "一级"),
            ("钦州浦北县中医院", "中医医院", "钦州", "一级"),
            ("贵港市卫生学校附属医院", "卫生院", "贵港", "一级"),
            ("玉林红十字医院", "综合医院", "玉林", "二级"),
            ("来宾市人民医院", "综合医院", "来宾", "二级"),
            ("河池市人民医院", "综合医院", "河池", "二级"),
            ("崇左市卫生计生委医院", "综合医院", "崇左", "二级"),
            
            # 防控机构
            ("广西疾病预防控制中心", "疾控中心", "广西", "二级"),
            ("南宁市疾病预防控制中心", "疾控中心", "南宁", "一级"),
            ("柳州市疾病预防控制中心", "疾控中心", "柳州", "一级"),
        ]
        
        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor()
            
            # 清空旧的演示机构数据
            logger.info("清空旧的演示机构数据...")
            cursor.execute("DELETE FROM medical_institution WHERE region IN ('广西', '南宁', '柳州', '桂林', '梧州', '北海', '钦州', '贵港', '玉林', '来宾', '河池', '崇左')")
            conn.commit()
            
            # 插入演示机构
            logger.info(f"正在插入 {len(demo_institutions)} 家医疗机构...\n")
            
            for name, type_, region, level in demo_institutions:
                cursor.execute("""
                    INSERT INTO medical_institution 
                    (name, type, region, level)
                    VALUES (%s, %s, %s, %s)
                """, (name, type_, region, level))
                logger.info(f"✅ {region:6} | {name:30} | {type_:10} | {level}")
            
            conn.commit()
            
            # 统计结果
            cursor.execute("SELECT COUNT(*) as cnt FROM medical_institution")
            total_count = cursor.fetchone()[0]
            
            logger.info("\n" + "=" * 60)
            logger.info(f"✅ 医疗机构初始化完成")
            logger.info(f"✅ 系统中现有医疗机构: {total_count} 家")
            logger.info("=" * 60)
        
        except Exception as e:
            logger.error(f"❌ 生成机构数据失败: {e}")
        finally:
            if conn:
                conn.close()

    def generate_demo_metrics(self):
        """生成演示的健康统计指标数据"""
        
        logger.info("=" * 60)
        logger.info("正在生成演示统计数据...")
        logger.info("=" * 60)
        
        # 演示数据结构：报告类别 -> 指标列表
        demo_reports = {
            "2024年全国医疗卫生综合统计": [
                ("医疗卫生机构总数", "1023458", "家"),
                ("其中：医院数", "34568", "家"),
                ("基层医疗卫生机构", "978234", "个"),
                ("专业公共卫生机构", "10656", "个"),
                ("卫生床位总数", "8945234", "张"),
                ("其中：医院床位", "7234567", "张"),
                ("基层卫生机构床位", "1678234", "张"),
                ("卫生人员总数", "12345678", "人"),
                ("其中：医生", "4567890", "人"),
                ("护士", "5432100", "人"),
                ("其他健康相关人员", "2345688", "人"),
            ],
            "2024年国家公共卫生服务": [
                ("建立健康档案人数", "89456789", "人"),
                ("老年人管理数", "12345678", "人"),
                ("高血压患者管理数", "23456789", "人"),
                ("糖尿病患者管理数", "12345678", "人"),
                ("严重精神障碍患者数", "567890", "人"),
                ("结核病患者数", "789012", "人"),
                ("传染病报告数", "234567", "例"),
            ],
            "2024年卫生投入与资源分配": [
                ("卫生总支出", "8.9万亿", "元"),
                ("政府卫生支出", "2.3万亿", "元"),
                ("社会卫生支出", "1.8万亿", "元"),
                ("个人卫生支出", "4.8万亿", "元"),
                ("人均卫生支出", "6234", "元"),
                ("卫生投入占GDP比重", "8.9", "%"),
            ],
            "2024年医疗保障覆盖률": [
                ("基本医保参保人数", "13.5亿", "人"),
                ("其中：城镇职工", "2.34亿", "人"),
                ("城乡居民", "11.16亿", "人"),
                ("医保覆盖率", "99.8", "%"),
                ("门诊均次费用", "156.78", "元"),
                ("住院平均费用", "12345", "元"),
                ("住院报销比例", "75.6", "%"),
            ],
        }
        
        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor(dictionary=True)
            
            # 清空旧数据
            logger.info("清空旧数据...")
            cursor.execute("DELETE FROM report_metrics")
            cursor.execute("""
                DELETE FROM national_news 
                WHERE source_category LIKE '%演示%'
            """)
            conn.commit()
            
            # 插入演示报告和指标
            report_index = 0
            for report_title, metrics in demo_reports.items():
                report_index += 1
                logger.info(f"\n📄 创建报告: {report_title}")
                
                # 插入或获取报告
                cursor.execute("""
                    SELECT id FROM national_news 
                    WHERE title = %s AND source_category = '演示数据'
                """, (report_title,))
                
                existing = cursor.fetchone()
                if existing:
                    report_id = existing['id']
                    logger.info(f"   使用已存在的报告ID: {report_id}")
                    # 清空该报告的旧指标
                    cursor.execute("DELETE FROM report_metrics WHERE report_id = %s", (report_id,))
                else:
                    # 插入新报告
                    fake_link = f"demo://report/{report_index}"
                    cursor.execute("""
                        INSERT INTO national_news 
                        (title, link, source_category, publish_date)
                        VALUES (%s, %s, %s, NOW())
                    """, (report_title, fake_link, "演示数据"))
                    conn.commit()
                    report_id = cursor.lastrowid
                    logger.info(f"   创建新报告ID: {report_id}")
                
                # 插入指标
                for metric_name, metric_value, unit in metrics:
                    cursor.execute("""
                        INSERT INTO report_metrics 
                        (report_id, metric_name, metric_value, unit)
                        VALUES (%s, %s, %s, %s)
                    """, (report_id, metric_name, metric_value, unit))
                    logger.info(f"   ✅ {metric_name}: {metric_value} {unit}")
                
                conn.commit()
            
            logger.info("\n" + "=" * 60)
            logger.info("数据统计:")
            
            # 统计结果
            cursor.execute("SELECT COUNT(*) as cnt FROM national_news WHERE source_category = '演示数据'")
            report_count = cursor.fetchone()['cnt']
            
            cursor.execute("SELECT COUNT(*) as cnt FROM report_metrics")
            metric_count = cursor.fetchone()['cnt']
            
            logger.info(f"✅ 创建演示报告: {report_count} 个")
            logger.info(f"✅ 创建统计指标: {metric_count} 条")
            logger.info("=" * 60)
        
        except Exception as e:
            logger.error(f"❌ 生成数据失败: {e}")
        finally:
            if conn:
                conn.close()

    def run(self):
        try:
            self.generate_demo_institutions()
            self.generate_demo_metrics()
        except Exception as e:
            logger.error(f"❌ 错误: {e}")


if __name__ == "__main__":
    generator = DemoDataGenerator()
    generator.run()
