# db_handler.py
import mysql.connector
import pandas as pd
import re
from data_handler import read_excel_fast

# ------------------ 数据库配置 ------------------
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'qwer.1234',
    'database': 'excel_compare',
    'charset': 'utf8mb4'
}


# =========================================================
# 基础初始化
# =========================================================
def init_database():
    """创建库、删旧表"""
    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        cursor.execute(f"USE {DB_CONFIG['database']}")
        cursor.execute("DROP TABLE IF EXISTS temp_table1")
        cursor.execute("DROP TABLE IF EXISTS temp_table2")
        cursor.execute("DROP TABLE IF EXISTS temp_mapping_table")
        conn.close()
        return True
    except Exception as e:
        print(f"数据库初始化失败: {str(e)}")
        return False


def sanitize_column_name(col_name):
    """把任意列名变成合法 MySQL 列名"""
    clean = re.sub(r'[^\w]', '_', str(col_name))
    if clean and clean[0].isdigit():
        clean = 'col_' + clean
    return clean[:64] or 'unnamed_column'


# =========================================================
# 表与数据导入
# =========================================================
def import_excel_to_db(file_path, sheet_name, table_name, is_file1=True, skip_rows=0, chunk_size=5000):
    """把 Excel 分块写入 MySQL"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(f"USE {DB_CONFIG['database']}")

        df = read_excel_fast(file_path, sheet_name, is_file1=is_file1,
                             skip_rows=skip_rows, chunk_size=chunk_size)

        if df.empty:
            conn.close()
            return 0

        df.columns = [sanitize_column_name(c) for c in df.columns]

        # 建表
        create_sql = _generate_create_table_sql(df, table_name)
        cursor.execute(create_sql)

        # 分块插入
        total_rows = len(df)
        for start in range(0, total_rows, chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            _insert_data(cursor, table_name, chunk)
            conn.commit()

        conn.close()
        return total_rows
    except Exception as e:
        raise Exception(f"导入Excel到数据库失败: {str(e)}")

def prepare_asset_category_mapping(rules, rule_file):
    """
          预先准备资产分类映射表数据
          """
    # 检查是否有资产分类字段需要对比
    has_asset_category = any(field_name == "资产分类" for field_name in rules.keys())
    if not has_asset_category:
        return False
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(f"USE {DB_CONFIG['database']}")
        conn.autocommit = True
        # 加载资产分类映射表
        mapping_df = _load_asset_category_mapping(rule_file)
        if mapping_df.empty or '同源目录完整名称' not in mapping_df.columns or '同源目录编码' not in mapping_df.columns:
            return False
        # 创建临时映射表
        create_mapping_table_sql = """
              CREATE TABLE temp_mapping_table (
                  `同源目录完整名称` VARCHAR(255),
                  `同源目录编码` VARCHAR(50)
              )
              """
        cursor.execute(create_mapping_table_sql)
        # 批量插入映射数据
        if not mapping_df.empty:
            # 准备批量插入数据
            insert_data = []
            for _, row in mapping_df.iterrows():
                try:
                    insert_data.append((str(row['同源目录完整名称']), str(row['同源目录编码'])))
                except:
                    continue

            if insert_data:
                insert_sql = """
                      INSERT INTO temp_mapping_table (`同源目录完整名称`, `同源目录编码`)
                      VALUES (%s, %s)
                      """
                # 分批插入，避免数据量过大
                batch_size = 1000
                for i in range(0, len(insert_data), batch_size):
                    batch = insert_data[i:i + batch_size]
                    cursor.executemany(insert_sql, batch)
                    conn.commit()
        conn.close()
        return True
    except Exception as e:
        raise Exception(f"准备资产分类映射表时出错: {str(e)}")

def _load_asset_category_mapping(rule_file):
    """
    从规则文件中加载资产分类映射表
    """
    try:
        # 读取规则文件中的"资产分类映射表"页签，跳过第一行
        mapping_df = pd.read_excel(rule_file, sheet_name='资产分类映射表', skiprows=1)
        return mapping_df
    except Exception as e:
        raise Exception(f"读取资产分类映射表失败: {str(e)}")

def _generate_create_table_sql(df, table_name):
    cols = [f"`{col}` LONGTEXT" for col in df.columns]
    sql = f"""
    CREATE TABLE `{table_name}` (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(cols)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    return sql


def _insert_data(cursor, table_name, df):
    if df.empty:
        return
    cols = [f"`{c}`" for c in df.columns]
    placeholders = ",".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO `{table_name}` ({','.join(cols)}) VALUES ({placeholders})"

    # 判断是否为表二
    is_table2 = table_name == 'temp_table2'

    processed_data = []
    for _, row in df.iterrows():
        processed_row = []
        for i, col_name in enumerate(df.columns):
            value = row[col_name]
            # 如果是表二且字段名包含"折旧"，则取绝对值
            if is_table2 and "折旧" in col_name:
                try:
                    # 尝试将值转换为数值并取绝对值
                    if pd.notna(value):
                        numeric_value = float(value)
                        processed_row.append(str(abs(numeric_value)))
                    else:
                        processed_row.append(None)
                except (ValueError, TypeError):
                    # 如果转换失败，保持原始值
                    processed_row.append(str(value) if pd.notna(value) else None)
            else:
                processed_row.append(str(value) if pd.notna(value) else None)
        processed_data.append(tuple(processed_row))

    cursor.executemany(sql, processed_data)


# =========================================================
# 通用查询
# =========================================================
# 修改 db_handler.py 中的 execute_query 方法
def execute_query(query, params=None, executemany=False):
    """执行 SQL 并返回 DataFrame"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        if params:
            if executemany:
                cursor.executemany(query, params)
            else:
                cursor.execute(query, params)
        else:
            cursor.execute(query)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        conn.close()
        return df
    except Exception as e:
        raise Exception(f"执行查询失败: {str(e)}")



# =========================================================
# 主键相关工具
# =========================================================
def create_compare_index(table: str, pk_cols: list):
    """给 _pk_concat 建索引"""
    idx_name = f"idx_{table}_pk"
    col_str = ",".join([f"`{c}`" for c in pk_cols])
    sql = f"ALTER TABLE `{table}` ADD UNIQUE INDEX {idx_name} ({col_str})"
    try:
        execute_query(sql)
    except Exception:
        pass  # 已存在


def add_concat_pk_column(table: str, expr: str):
    """给表增加 _pk_concat 列并填充"""
    try:
        execute_query(f"ALTER TABLE `{table}` ADD COLUMN `_pk_concat` VARCHAR(255)")
    except Exception:
        pass
    execute_query(f"UPDATE `{table}` SET `_pk_concat` = {expr}")


def fetch_rows_by_pk(table: str, pk_cols: list, wanted_keys: set):
    """根据 _pk_concat 拉取行"""
    if not wanted_keys:
        return pd.DataFrame()
    keys = list(wanted_keys)
    placeholders = ",".join(["%s"] * len(keys))
    sql = f"SELECT * FROM `{table}` WHERE _pk_concat IN ({placeholders})"
    return execute_query(sql, params=keys)


# =========================================================
# 清理
# =========================================================
def drop_tables():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS temp_table1")
        cursor.execute("DROP TABLE IF EXISTS temp_table2")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"删除表失败: {str(e)}")