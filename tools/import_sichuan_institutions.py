"""
四川省医院机构信息Excel导入脚本
功能：读取Excel文件，将医疗机构信息导入到medical_institution表
"""

import openpyxl
import mysql.connector
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SichuanInstitutionImporter:
    def __init__(self):
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "rootpassword",
            "database": "health_db"
        }

    def connect_db(self):
        """连接数据库"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            return conn
        except mysql.connector.Error as err:
            logger.error(f"数据库连接失败: {err}")
            raise

    def read_excel(self, file_path):
        """
        读取Excel文件
        :param file_path: Excel文件路径
        :return: 数据列表
        """
        logger.info(f"正在读取Excel文件: {file_path}")
        
        wb = openpyxl.load_workbook(file_path, read_only=True)
        ws = wb.active
        
        data_list = []
        
        # 使用enumerate迭代所有行
        row_iterator = list(ws.iter_rows(values_only=True))
        
        for row_idx, row in enumerate(row_iterator):
            # Row 1: 空行，Row 2: 标题，Row 3: 表头，Row 4+: 数据
            if row_idx < 3:
                continue
            
            # 跳过空行（第一列为空）
            if row[0] is None and row[1] is None:
                continue
            
            # 解析数据行
            row_data = {
                '序号': row[0] if len(row) > 0 else None,
                'name': row[1] if len(row) > 1 else None,           # 医疗机构名称
                'type': row[2] if len(row) > 2 else None,           # 医疗机构类别
                'city': row[3] if len(row) > 3 else None,           # 市
                'district': row[4] if len(row) > 4 else None,       # 县/区/市
                'address': row[5] if len(row) > 5 else None,        # 地址
                'license': row[6] if len(row) > 6 else None,        # 执业许可证登记号
                'level': row[7] if len(row) > 7 else None,          # 医院等级分类
                'level_code': row[8] if len(row) > 8 else None,     # 医院等级分类编码
                'approval': row[9] if len(row) > 9 else None,       # 批准文号
                'phone': row[10] if len(row) > 10 else None,        # 联系电话
            }
            
            # 拼接地区：市 + 县/区/市
            region_parts = []
            if row_data['city']:
                region_parts.append(str(row_data['city']).strip())
            if row_data['district']:
                region_parts.append(str(row_data['district']).strip())
            row_data['region'] = ''.join(region_parts) if region_parts else None
            
            data_list.append(row_data)
        
        wb.close()
        logger.info(f"读取完成，共 {len(data_list)} 条数据")
        return data_list

    def import_to_db(self, data_list):
        """
        将数据导入数据库
        :param data_list: 数据列表
        """
        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor()
            
            inserted_count = 0
            skipped_count = 0
            error_count = 0
            
            for idx, data in enumerate(data_list, 1):
                name = data['name']
                type_ = data['type']
                region = data['region']
                level = data['level']
                
                # 跳过没有名称的记录
                if not name:
                    logger.warning(f"跳过第 {idx} 条：医疗机构名称为空")
                    skipped_count += 1
                    continue
                
                # 清洗数据
                name = str(name).strip() if name else None
                type_ = str(type_).strip() if type_ else None
                region = str(region).strip() if region else None
                level = str(level).strip() if level else None
                
                try:
                    # 插入数据
                    sql = """
                        INSERT INTO medical_institution 
                        (name, type, region, level)
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(sql, (name, type_, region, level))
                    inserted_count += 1
                    
                    if idx <= 5 or idx % 100 == 0:
                        logger.info(f"[{idx}/{len(data_list)}] {name} - {type_} - {region} - {level}")
                    
                except mysql.connector.Error as e:
                    if "Duplicate entry" in str(e):
                        logger.debug(f"重复记录，跳过: {name}")
                        skipped_count += 1
                    else:
                        logger.error(f"插入失败: {name}, 错误: {e}")
                        error_count += 1
            
            conn.commit()
            
            logger.info("\n" + "=" * 60)
            logger.info("导入完成！")
            logger.info(f"   成功插入: {inserted_count} 条")
            logger.info(f"   跳过（重复/空值）: {skipped_count} 条")
            logger.info(f"   错误: {error_count} 条")
            logger.info("=" * 60)
            
            # 验证导入结果
            cursor.execute("SELECT COUNT(*) as cnt FROM medical_institution")
            total_count = cursor.fetchone()[0]
            logger.info(f"数据库中现有医疗机构总数: {total_count}")
            
        except Exception as e:
            logger.error(f"导入失败: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def run(self, file_path):
        """执行导入"""
        data_list = self.read_excel(file_path)
        self.import_to_db(data_list)


if __name__ == "__main__":
    import os
    
    # Excel文件路径
    excel_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                               'inputs', 
                               '四川省医院机构信息（截至2025年12月）.xlsx')
    
    if not os.path.exists(excel_file):
        logger.error(f"文件不存在: {excel_file}")
    else:
        importer = SichuanInstitutionImporter()
        importer.run(excel_file)