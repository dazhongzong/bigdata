import pyodbc
import pandas as pd
import numpy as np
import json
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import os
import argparse
class ExcelToSQLImporter:
    """Excel数据导入SQL Server的工具类（支持JSON配置）"""
    
    def __init__(self, config: Dict):
        """
        从配置字典初始化参数
        
        :param config: 包含数据库连接和导入设置的字典
        """
        # 数据库连接参数
        sql_config = config["sql_server"]
        self.server = sql_config["server"]
        self.database = sql_config["database"]
        self.username = sql_config["username"]
        self.password = sql_config["password"]
        self.port = sql_config.get("port", 1433)  # 默认为1433
        
        # 导入设置
        import_config = config["import_settings"]
        self.chunk_size = import_config.get("chunk_size", 1000)
        self.if_exists = import_config.get("if_exists", "fail")
        # self.primary_key = import_config.get("primary_key")
        
        # 初始化连接和游标
        self.conn = None
        self.cursor = None
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _get_connection_string(self) -> str:
        """生成SQL Server连接字符串"""
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes"
        )

    def connect(self) -> None:
        try:
            self.conn = pyodbc.connect(self._get_connection_string())
            self.cursor = self.conn.cursor()
            self.cursor.fast_executemany = True  # 🔥 核心提速开关
            self.logger.info(f"成功连接到 {self.server}/{self.database}")
        except pyodbc.Error as e:
            self.logger.error(f"连接失败: {str(e)}")
            raise

    def close(self) -> None:
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.logger.info("数据库连接已关闭")

    def _map_data_type(self, pandas_dtype) -> str:
        """映射pandas数据类型到SQL Server类型"""
        if pd.api.types.is_integer_dtype(pandas_dtype):
            return "INT"
        elif pd.api.types.is_float_dtype(pandas_dtype):
            return "DECIMAL(18, 4)"
        elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
            return "DATETIME"
        elif pd.api.types.is_bool_dtype(pandas_dtype):
            return "BIT"
        else:
            return "NVARCHAR(255)"

    def _create_table_sql(self, table_name: str, df: pd.DataFrame) -> str:
        """生成创建表的SQL语句"""
        columns = []
        for col in df.columns:
            sql_col = col
            sql_type = self._map_data_type(df[col].dtype)
            columns.append(f"[{sql_col}] {sql_type}")
        
        # 主键约束
        #primary_key_sql = f", PRIMARY KEY ({self.primary_key})" if self.primary_key else ""
        
        return f"""
        CREATE TABLE [{table_name}] (
            {', '.join(columns)}
        )
        """.strip()

    def _prepare_data(self, df: pd.DataFrame) -> List[Tuple]:
        """预处理数据（处理空值和日期格式）"""
        df = df.replace({np.nan: None})
        
        # 转换日期格式
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col].dtype):
                df[col] = df[col].apply(
                    lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(x) else None
                )
        
        return [tuple(row) for row in df.itertuples(index=False, name=None)]

    def import_excel(self, excel_path: str, table_name: str, sheet_name: str = 0) -> None:
        """导入Excel数据到SQL Server"""
        try:
            # 读取Excel
            self.logger.info(f"读取Excel: {excel_path} (工作表: {sheet_name})")
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            self.logger.info(f"读取完成，共 {len(df)} 行")

            # 处理列名
            df.columns = [col for col in df.columns]

            # 检查表是否存在
            self.cursor.execute(
                "SELECT COUNT(*) FROM bigdata.sys.tables WHERE name = ?", (table_name,)
            )
            table_exists = self.cursor.fetchone()[0] == 1

            # 处理表存在的情况
            if table_exists:
                if self.if_exists == "fail":
                    self.logger.error(f"表 {table_name} 已存在，终止导入")
                    return
                elif self.if_exists == "replace":
                    self.logger.info(f"清空表 {table_name}")
                    self.cursor.execute(f"TRUNCATE TABLE [{table_name}]")
                    self.conn.commit()
                elif self.if_exists == "append":
                    self.logger.info(f"表 {table_name} 已存在，追加数据")
                elif self.if_exists == "delete":
                    self.logger.info(f"删除表 {table_name}")
                    self.cursor.execute(f"DROP TABLE [{table_name}]")
                    # 创建新表
                    self.logger.info(f"创建表 {table_name}")
                    self.cursor.execute(self._create_table_sql(table_name, df))
                    self.conn.commit()
                else:
                    raise ValueError(f"未知的 if_exists 参数: {self.if_exists}")
            else:
                # 创建新表
                self.logger.info(f"创建表 {table_name}")
                self.cursor.execute(self._create_table_sql(table_name, df))
                self.conn.commit()

            # 数据预处理
            data = self._prepare_data(df)
            if not data:
                self.logger.warning("无数据可导入")
                return

            # 批量插入
            self.logger.info(f"开始导入，批次大小: {self.chunk_size}")
            columns = ", ".join([f"[{col}]" for col in df.columns])
            placeholders = ", ".join(["?" for _ in df.columns])
            
            insert_sql = f"INSERT INTO [{table_name}] ({columns}) VALUES ({placeholders})"

            # 分批次执行
            total = len(data)
            for i in range(0, total, self.chunk_size):
                chunk = data[i:i+self.chunk_size]
                self.cursor.executemany(insert_sql, chunk)
                self.conn.commit()
                self.logger.info(f"进度: {min(i+self.chunk_size, total)}/{total} 行")

            self.logger.info(f"导入成功，共 {total} 行")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"导入失败: {str(e)}")
            raise

def load_config(config_path: str = "./config.json") -> Dict:
    """
    从JSON文件加载配置
    
    :param config_path: JSON配置文件路径
    :return: 配置字典
    """
    
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"配置文件 {config_path} 不存在")
    except json.JSONDecodeError:
        raise ValueError(f"配置文件 {config_path} 格式错误")


def validate_excel(file_path: str) -> bool:
    """验证Excel文件有效性"""
    try:
        pd.read_excel(file_path, nrows=1)
        return True
    except Exception as e:
        logging.error(f"Excel验证失败: {str(e)}")
        return False
def load_table(config,excel_path:str,table_name=None,sheet_name=0) -> None:
    # 3. 执行导入
    if validate_excel(excel_path):
        importer = ExcelToSQLImporter(config)
        try:
            importer.connect()
            importer.import_excel(
                excel_path=excel_path,
                table_name=table_name,
                sheet_name=sheet_name
            )
        finally:
            importer.close()
def load_Fact_Table(config,excel_path:str,table_name=None) -> None:
    
    # 2. 配置导入参数
    if table_name is None:
        table_name = os.path.basename(excel_path)
    sheet_name = 0  # 第一个工作表
    
    load_table(config,excel_path=excel_path,table_name=table_name,sheet_name=sheet_name)
def load_Dim_Table(config,excel_path:str) -> None:
    if os.path.isdir(excel_path):
        files = []
        for file in os.listdir(excel_path):
            if file.endswith(".xlsx"):
                file = os.path.join(excel_path, file)
                files.append(file)
    sheet_name = 0 
    for file in files:
        # 3. 执行导入
        table_name = os.path.basename(file).split(".")[0]
        load_table(config,excel_path=file,table_name=table_name,sheet_name=sheet_name)

if __name__ == "__main__":

    # 1. 加载配置
    config = load_config()
    tables_config = config["tables"]
    if tables_config.get("dim_table",True):
        load_Dim_Table(config,excel_path="./data/dim")
    if tables_config.get("fact_table",True):
        load_Fact_Table(config,excel_path="./output/output.xlsx",table_name="Fact_FlightTicket")