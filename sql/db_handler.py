# db_handler.py
import mysql.connector
import pandas as pd
import re
from data_handler import read_excel_fast

# 数据库配置 - 请根据实际情况修改
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'qwer.1234',  # 请修改为实际密码
    'database': 'excel_compare',
    'charset': 'utf8mb4'
}


def init_database():
    """初始化数据库，创建数据库和表"""
    try:
        # 连接MySQL服务器（不指定数据库）
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = conn.cursor()

        # 创建数据库（如果不存在）
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        cursor.execute(f"USE {DB_CONFIG['database']}")

        # 删除旧表（如果存在）
        cursor.execute("DROP TABLE IF EXISTS temp_table1")
        cursor.execute("DROP TABLE IF EXISTS temp_table2")

        conn.close()
        return True
    except Exception as e:
        print(f"数据库初始化失败: {str(e)}")
        return False


def sanitize_column_name(col_name):
    """清理列名，使其符合MySQL命名规范"""
    # 移除特殊字符，只保留字母、数字和下划线
    clean_name = re.sub(r'[^\w]', '_', str(col_name))
    # 确保不以数字开头
    if clean_name and clean_name[0].isdigit():
        clean_name = 'col_' + clean_name
    # 限制长度
    clean_name = clean_name[:64]
    # 如果为空，提供默认名称
    if not clean_name:
        clean_name = 'unnamed_column'
    return clean_name


def import_excel_to_db(file_path, sheet_name, table_name, is_file1=True, skip_rows=0, chunk_size=5000):
    """将Excel文件分块导入到MySQL数据库"""
    try:
        # 初始化数据库连接
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 使用数据库
        cursor.execute(f"USE {DB_CONFIG['database']}")

        # 使用 read_excel_fast 读取整个文件
        df = read_excel_fast(
            file_path,
            sheet_name,
            is_file1=is_file1,
            skip_rows=skip_rows,
            chunk_size=chunk_size
        )

        # 清理列名
        df.columns = [sanitize_column_name(col) for col in df.columns]

        # 如果 DataFrame 不为空，则创建表并插入数据
        if not df.empty:
            # 创建表结构
            create_table_sql = generate_create_table_sql(df, table_name)
            cursor.execute(create_table_sql)

            # 分块插入数据以避免内存问题
            total_rows = len(df)
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                insert_data(cursor, table_name, chunk)
                conn.commit()

            conn.close()
            return total_rows
        else:
            conn.close()
            return 0

    except Exception as e:
        raise Exception(f"导入Excel到数据库失败: {str(e)}")


def generate_create_table_sql(df, table_name):
    """根据DataFrame生成创建表的SQL语句"""
    columns = []
    for col in df.columns:
        # 简化处理，所有字段都用LONGTEXT类型以避免类型转换问题
        columns.append(f"`{col}` LONGTEXT")

    # 添加自增主键
    sql = f"""
    CREATE TABLE `{table_name}` (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(columns)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    return sql


def insert_data(cursor, table_name, df):
    """将DataFrame数据插入到MySQL表中"""
    if df.empty:
        return

    # 准备列名
    columns = [f"`{col}`" for col in df.columns]

    # 准备占位符
    placeholders = ', '.join(['%s'] * len(df.columns))

    # 准备SQL
    sql = f"INSERT INTO `{table_name}` ({', '.join(columns)}) VALUES ({placeholders})"

    # 准备数据
    data = []
    for _, row in df.iterrows():
        # 将所有值转换为字符串，处理空值
        row_data = tuple(str(val) if pd.notna(val) else None for val in row)
        data.append(row_data)

    # 批量插入
    cursor.executemany(sql, data)


def execute_query(query, params=None):
    """执行SQL查询并返回结果"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # 获取列名
        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        # 获取数据
        rows = cursor.fetchall()

        # 转换为DataFrame
        df = pd.DataFrame(rows, columns=columns)

        conn.close()
        return df
    except Exception as e:
        raise Exception(f"执行查询失败: {str(e)}")


def drop_tables():
    """删除临时表"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS temp_table1")
        cursor.execute("DROP TABLE IF EXISTS temp_table2")

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"删除表失败: {str(e)}")
