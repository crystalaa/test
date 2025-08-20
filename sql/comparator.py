import sys
import time
import traceback
import logging
import pandas as pd
import re
import gc
from PyQt5.QtCore import QThread, pyqtSignal
from rule_handler import read_enum_mapping, read_erp_combo_map
from db_handler import (
    init_database, import_excel_to_db, execute_query, drop_tables,
    create_compare_index, fetch_rows_by_pk, prepare_asset_category_mapping, _load_asset_category_mapping
)

TEMP_TABLE1 = 'temp_table1'
TEMP_TABLE2 = 'temp_table2'


class CompareWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)

    def __init__(self, file1, file2, rule_file, sheet_name1, sheet_name2,
                 primary_keys=None, rules=None, skip_rows=0, chunk_size=5000):
        super().__init__()
        self.file1 = file1
        self.file2 = file2
        self.rule_file = rule_file
        self.sheet_name1 = sheet_name1
        self.sheet_name2 = sheet_name2
        self.primary_keys = primary_keys if primary_keys else []
        self.rules = rules if rules else {}
        self.skip_rows = skip_rows
        self.chunk_size = chunk_size

        self.missing_assets = []
        self.diff_records = []
        self.summary = {}
        self.missing_rows = []
        self.extra_in_file2 = []
        self.diff_full_rows = []
        self.enum_map = read_enum_mapping(rule_file)
        self.erp_combo_map = read_erp_combo_map(rule_file)
        self.asset_code_to_original = {}

    # ---------- 工具 ----------
    @staticmethod
    def normalize_value(val):
        if pd.isna(val) or val is None or (isinstance(val, str) and str(val).strip() == ''):
            return ''
        return str(val).strip()

    def _normalize_text_value(self, value):
        """
        标准化文本值，将具有相同含义的不同表示转换为统一形式
        """
        if pd.isna(value) or value is None:
            return ''

        str_value = str(value).strip().upper()  # 转换为大写以便统一比较

        # 处理"是"的表示：是、Y、y
        if str_value in ['是', 'Y']:
            return '是'

        # 处理"否"的表示：否、N、n
        if str_value in ['否', 'N']:
            return '否'

        return str(value).strip()  # 其他情况返回原始值（保持原始大小写）

    def _normalize_depreciation_method(self, value, is_file1=True):
        """
        标准化折旧方法字段值
        如果是表二且值为"直线法"，则转换为"年限平均法"
        """
        if pd.isna(value) or value is None:
            return ''

        str_value = str(value).strip()

        # 对于表二，将"直线法"转换为"年限平均法"
        if not is_file1 and str_value == '直线法':
            return '年限平均法'

        return str(value).strip()

    def _extract_second_level(self, value):
        """
        从监管资产属性中提取二级分类
        支持两种格式：
        1. '输配电资产\省级电网资产' -> '省级电网资产'
        2. '电力常规资产-省级电网资产' -> '省级电网资产'
        """
        if not value or value.strip() == '':
            return ''

        # 处理反斜杠分隔的格式
        if '\\' in value:
            parts = value.split('\\')
            return parts[-1].strip() if len(parts) > 1 else value.strip()

        # 处理短横线分隔的格式
        if '-' in value:
            parts = value.split('-')
            return parts[-1].strip() if len(parts) > 1 else value.strip()

        # 如果没有分隔符，返回原值
        return value.strip()

    def calculate_field(self, df, calc_rule, data_type):
        if not calc_rule:
            return None
        try:
            if '[:' in calc_rule and ']' in calc_rule:
                field, length_str = calc_rule.split('[:')
                field = field.strip()
                length = int(length_str.strip(']').strip())
                if field not in df.columns:
                    raise Exception(f"字段不存在：{field}")
                return df[field].fillna('').astype(str).str[:length]

            if data_type == "文本":
                fields = [f.strip() for f in calc_rule.split('+')]
                missing = [f for f in fields if f not in df.columns]
                if missing:
                    raise Exception(f"表达式含不存在字段：{missing}")
                result = df[fields[0]].fillna('').astype(str)
                for f in fields[1:]:
                    result += df[f].fillna('').astype(str)
                return result

            if data_type == "数值":
                field_pattern = re.compile(r'[a-zA-Z\u4e00-\u9fa5]+')
                fields_in_rule = field_pattern.findall(calc_rule)
                missing = [f for f in fields_in_rule if f not in df.columns]
                if missing:
                    raise Exception(f"表达式含不存在字段：{missing}")
                df_num = df.copy()
                return df_num.eval(calc_rule)

            raise Exception(f"不支持的数据类型：{data_type}")
        except Exception as e:
            raise Exception(f"计算规则执行失败（{calc_rule}）：{str(e)}")

    # ---------- SQL 侧主键 ----------
    def _build_pk_expr(self, table_alias: str, is_file1: bool) -> str:
        """生成 _pk_concat 的 SQL 表达式"""
        # 先找主键字段对应的规则
        pk_field = None
        for f, r in self.rules.items():
            if r.get("is_primary"):
                pk_field = f
                break
        rule = self.rules.get(pk_field, {})
        calc_rule = rule.get("calc_rule")

        if is_file1:
            # 表一：primary_keys 里的列直接拼
            cols = [f"`{c}`" for c in self.primary_keys]
            return "CONCAT_WS(' + ', " + ",".join(cols) + ")"
        else:
            # 表二：有计算规则就转 SQL，否则用 table2_field
            if calc_rule:
                expr = re.sub(r'\+', ",", calc_rule)
                return f"CONCAT({expr})"
            else:
                col = rule.get("table2_field")
                if col:
                    return f"`{col}`"
                else:
                    raise Exception("规则文件中未给 ERP 表定义主键字段")

    def _build_field_expr(self, field_name, is_file1=True):
        """
        为指定字段生成SQL表达式
        """
        rule = self.rules.get(field_name, {})
        calc_rule = rule.get("calc_rule")

        if is_file1:
            # 表一：直接使用字段名
            return f"`{field_name}`"
        else:
            # 表二：如果有计算规则则转换为SQL表达式，否则使用table2_field或字段名
            if calc_rule:
                # 处理不同类型的计算规则

                # 检查是否是简单的字段截取规则，如 "字段[:10]"
                if '[:' in calc_rule and ']' in calc_rule:
                    field_part, length_str = calc_rule.split('[:')
                    field_part = field_part.strip()
                    length = int(length_str.strip(']').strip())
                    return f"LEFT(`{field_part}`, {length})"

                # 检查是否是文本拼接规则，如 "字段1+字段2"
                elif rule.get("data_type") == "文本" and re.match(r'^[a-zA-Z\u4e00-\u9fa50-9_+\s]+$', calc_rule):
                    # 只包含字段名、加号和空格的表达式认为是文本拼接
                    fields = [f.strip() for f in calc_rule.split('+') if f.strip()]
                    concat_fields = [f"`{f}`" for f in fields]
                    return f"CONCAT({', '.join(concat_fields)})"

                # 处理数值计算规则，如 "使用年限+使用期间/12"
                elif rule.get("data_type") == "数值":
                    # 将字段名加上反引号，支持四则运算
                    field_pattern = re.compile(r'[a-zA-Z\u4e00-\u9fa5][a-zA-Z\u4e00-\u9fa50-9_]*')
                    fields_in_rule = field_pattern.findall(calc_rule)

                    expr = calc_rule
                    # 替换字段名为带反引号的形式
                    for field in sorted(fields_in_rule, key=len, reverse=True):  # 从长到短替换，避免部分匹配
                        expr = expr.replace(field, f"`{field}`")
                    return expr

                else:
                    return f"`{field_name}`"
            else:
                # 没有计算规则，使用table2_field映射或默认字段名
                table2_field = rule.get("table2_field", field_name)
                return f"`{table2_field}`"

    def _add_concat_pk_column(self, table: str, expr: str):
        """给指定表增加 _pk_concat 列并填充"""
        try:
            execute_query(f"ALTER TABLE `{table}` ADD COLUMN `_pk_concat` VARCHAR(255)")
        except Exception:
            pass  # 列已存在
        execute_query(f"UPDATE `{table}` SET `_pk_concat` = {expr}")

    def _process_depreciation_fields(self, table):
        """
        处理表中包含"折旧"的数值字段，将其转换为绝对值
        """
        try:
            for field_name, rule in self.rules.items():
                # 如果是数值类型且字段名包含"折旧"
                if rule.get("data_type") == "数值" and "折旧" in field_name:
                    try:
                        # 更新表中的数据，对折旧字段取绝对值
                        execute_query(
                            f"UPDATE `{table}` SET `{field_name}` = ABS(IFNULL(`{field_name}`, 0)) WHERE `{field_name}` IS NOT NULL")
                    except Exception as e:
                        self.log_signal.emit(f"处理表 {table} 的字段 {field_name} 时出错: {str(e)}")
        except Exception as e:
            self.log_signal.emit(f"处理折旧字段时发生错误: {str(e)}")

    def _add_calculated_fields(self, table, is_file1=True):
        """
        为表添加计算字段
        """
        for field_name, rule in self.rules.items():
            calc_rule = rule.get("calc_rule")
            # 只处理有计算规则的字段，且只处理表二
            if calc_rule and not is_file1 and rule.get("data_type") == "数值":
                try:
                    expr = self._build_field_expr(field_name, is_file1=False)
                    # 添加计算字段列
                    execute_query(f"ALTER TABLE `{table}` ADD COLUMN `_calc_{field_name}` DECIMAL(20,4)")
                    # 如果是折旧相关字段，取绝对值
                    if "折旧" in field_name:
                        # 填充计算字段值，处理可能的除零错误，并取绝对值
                        execute_query(f"UPDATE `{table}` SET `_calc_{field_name}` = ABS(COALESCE({expr}, 0))")
                    else:
                        # 填充计算字段值，处理可能的除零错误
                        execute_query(f"UPDATE `{table}` SET `_calc_{field_name}` = COALESCE({expr}, 0)")
                except Exception as e:
                    # 列可能已存在，忽略错误
                    pass

            # 处理文本拼接计算规则
            elif calc_rule and not is_file1 and rule.get("data_type") == "文本":
                try:
                    expr = self._build_field_expr(field_name, is_file1=False)
                    # 添加计算字段列
                    execute_query(f"ALTER TABLE `{table}` ADD COLUMN `_calc_{field_name}` VARCHAR(255)")
                    # 填充计算字段值
                    execute_query(f"UPDATE `{table}` SET `_calc_{field_name}` = {expr}")
                except Exception as e:
                    # 列可能已存在，忽略错误
                    pass

    def _diff_by_mysql(self):
        """纯 SQL 完成交集/差集"""
        # 查询共同的主键
        common_sql = """
        SELECT GROUP_CONCAT(t1._pk_concat SEPARATOR '||') AS common_keys
        FROM temp_table1 t1
        INNER JOIN temp_table2 t2 ON t1._pk_concat = t2._pk_concat
        """

        # 查询在表1中存在但在表2中缺失的主键
        missing_sql = """
        SELECT GROUP_CONCAT(t1._pk_concat SEPARATOR '||') AS missing_keys
        FROM temp_table1 t1
        LEFT JOIN temp_table2 t2 ON t1._pk_concat = t2._pk_concat
        WHERE t2._pk_concat IS NULL
        """

        # 查询在表2中存在但表1中多余的主键
        extra_sql = """
        SELECT GROUP_CONCAT(t2._pk_concat SEPARATOR '||') AS extra_keys
        FROM temp_table2 t2
        LEFT JOIN temp_table1 t1 ON t2._pk_concat = t1._pk_concat
        WHERE t1._pk_concat IS NULL
        """

        common_result = execute_query(common_sql)
        missing_result = execute_query(missing_sql)
        extra_result = execute_query(extra_sql)

        # 合并结果
        result_df = pd.DataFrame({
            'common_keys': [common_result.iloc[0, 0] if not common_result.empty and common_result.iloc[0, 0] else ''],
            'missing_keys': [
                missing_result.iloc[0, 0] if not missing_result.empty and missing_result.iloc[0, 0] else ''],
            'extra_keys': [extra_result.iloc[0, 0] if not extra_result.empty and extra_result.iloc[0, 0] else '']
        })

        return result_df

    def _compare_fields_in_db(self, common_codes):
        """
        在数据库中对比字段差异
        """
        diff_conditions = []

        # 为每个字段构建差异条件
        for field_name, rule in self.rules.items():
            if rule.get("is_primary"):
                continue  # 跳过主键字段

            data_type = rule.get("data_type", "文本")
            tail_diff = rule.get("tail_diff", 0)

            # 表一字段名
            src_field = f"`t1`.`{field_name}`"

            # 表二字段：如果有计算规则则使用计算字段，否则使用映射字段
            if rule.get("calc_rule") and rule.get("data_type") in ["数值", "文本"]:
                tgt_field = f"`t2`.`_calc_{field_name}`"
            else:
                table2_field = rule.get("table2_field", field_name)
                tgt_field = f"`t2`.`{table2_field}`"

            # 根据数据类型构建差异条件，考虑空值情况
            if data_type == "数值":
                if "折旧" in field_name:
                    condition = f"NOT (IFNULL({src_field}, '') = '' AND IFNULL({tgt_field}, '') = '') AND ABS(IFNULL({src_field}, 0)) != ABS(IFNULL({tgt_field}, 0))"
                    if float(tail_diff) > 0:
                        condition = f"NOT (IFNULL({src_field}, '') = '' AND IFNULL({tgt_field}, '') = '') AND ABS(ABS(IFNULL({src_field}, 0)) - ABS(IFNULL({tgt_field}, 0))) > {tail_diff}"
                else:
                    # 修改数值比较逻辑，增加精度处理
                    condition = f"NOT (IFNULL({src_field}, '') = '' AND IFNULL({tgt_field}, '') = '') AND IFNULL({src_field}, 0) != IFNULL({tgt_field}, 0)"
                    if float(tail_diff) > 0:
                        # 使用ROUND函数处理精度，确保比较时两边有相同精度
                        rounded_src = f"ROUND(IFNULL({src_field}, 0), {tail_diff})"
                        rounded_tgt = f"ROUND(IFNULL({tgt_field}, 0), {tail_diff})"
                        condition = f"NOT (IFNULL({src_field}, '') = '' AND IFNULL({tgt_field}, '') = '') AND ABS({rounded_src} - {rounded_tgt}) > {tail_diff}"
                diff_conditions.append(condition)

            elif data_type == "日期":
                # 统一日期格式进行比较
                condition = f"NOT (IFNULL({src_field}, '') = '' AND IFNULL({tgt_field}, '') = '') AND DATE_FORMAT(STR_TO_DATE(IFNULL({src_field}, ''), '%Y-%m-%d'), '%Y-%m-%d') != DATE_FORMAT(STR_TO_DATE(IFNULL({tgt_field}, ''), '%Y-%m-%d'), '%Y-%m-%d')"
                diff_conditions.append(condition)

            elif data_type == "文本":
                # 特殊处理资产分类字段
                if field_name == "资产分类":
                    # 对于资产分类，比较前两位编码（使用预先准备好的临时表）
                    # 表二实际用于对比的字段是"资产明细类别"
                    table2_field = "资产明细类别"
                    condition = f"""
                    NOT (IFNULL({src_field}, '') = '' AND IFNULL(t2.`{table2_field}`, '') = '') 
                    AND (
                        LEFT(
                            IFNULL(
                                (SELECT m.`同源目录编码` 
                                 FROM temp_mapping_table m 
                                 WHERE m.`同源目录完整名称` = {src_field} 
                                 LIMIT 1), 
                                {src_field}
                            ), 
                            2
                        ) != LEFT(IFNULL(t2.`{table2_field}`, ''), 2)
                    )
                    """
                    diff_conditions.append(condition)
                # 对于折旧方法字段，需要特殊处理表二中的"直线法"视为"年限平均法"
                elif "折旧方法" in field_name:
                    # 在SQL中处理：如果表二字段是"直线法"，则替换为"年限平均法"进行比较
                    adjusted_tgt_field = f"CASE WHEN TRIM(IFNULL({tgt_field}, '')) = '直线法' THEN '年限平均法' ELSE TRIM(IFNULL({tgt_field}, '')) END"
                    condition = f"NOT (IFNULL({src_field}, '') = '' AND IFNULL({tgt_field}, '') = '') AND TRIM(IFNULL({src_field}, '')) != {adjusted_tgt_field}"
                else:
                    condition = f"NOT (IFNULL({src_field}, '') = '' AND IFNULL({tgt_field}, '') = '') AND TRIM(IFNULL({src_field}, '')) != TRIM(IFNULL({tgt_field}, ''))"
                diff_conditions.append(condition)

            else:
                condition = f"NOT (IFNULL({src_field}, '') = '' AND IFNULL({tgt_field}, '') = '') AND IFNULL({src_field}, '') != IFNULL({tgt_field}, '')"
                diff_conditions.append(condition)

        if not diff_conditions:
            return []  # 没有需要对比的字段

        # 构建主键选择表达式
        pk_fields_src = [f"`t1`.`{pk}`" for pk in self.primary_keys]
        pk_fields_tgt = [f"`t2`.`{pk}`" for pk in self.primary_keys]

        # 构建所有需要返回的字段列表
        all_fields = list(self.rules.keys())

        # 构建完整SQL
        select_fields = []

        # 添加主键字段
        select_fields.append("t1._pk_concat")
        select_fields.extend(pk_fields_src)
        select_fields.extend(pk_fields_tgt)

        # 添加源表字段
        for f in all_fields:
            if not self.rules[f].get('is_primary'):
                select_fields.append(f"t1.`{f}` as src_{f}")

        # 添加目标表字段（正确处理字段映射）
        for f in all_fields:
            if not self.rules[f].get('is_primary'):
                rule = self.rules[f]
                if rule.get("calc_rule") and rule.get("data_type") in ["数值", "文本"]:
                    # 使用计算字段
                    select_fields.append(f"`t2`.`_calc_{f}` as tgt_{f}")
                else:
                    # 使用映射字段
                    table2_field = rule.get("table2_field", f)
                    select_fields.append(f"t2.`{table2_field}` as tgt_{f}")

        # 特别确保资产明细类别字段被包含在查询结果中
        if "资产分类" in self.rules:
            select_fields.append("t2.`资产明细类别` as tgt_资产明细类别")

        sql = f"""
        SELECT 
            {', '.join(select_fields)}
        FROM {TEMP_TABLE1} t1
        INNER JOIN {TEMP_TABLE2} t2 ON t1._pk_concat = t2._pk_concat
        WHERE {' OR '.join([f'({cond})' for cond in diff_conditions])}
        """

        try:
            result_df = execute_query(sql)
            diff_records = []

            if not result_df.empty:
                for _, row in result_df.iterrows():
                    src_data = {}
                    tgt_data = {}

                    # 主键字段
                    for pk in self.primary_keys:
                        src_data[pk] = row[pk]
                        tgt_data[pk] = row[f"{pk}"]  # 注意这里可能需要调整列名

                    # 其他字段
                    for field in all_fields:
                        if not self.rules[field].get("is_primary"):
                            src_data[field] = row[f"src_{field}"]
                            tgt_data[field] = row[f"tgt_{field}"]

                    # 特别处理资产明细类别字段
                    if "资产分类" in self.rules:
                        tgt_data["资产明细类别"] = row["tgt_资产明细类别"]

                    diff_records.append({
                        "source": src_data,
                        "target": tgt_data
                    })

            return diff_records

        except Exception as e:
            self.log_signal.emit(f"数据库对比出错：{str(e)}")
            return []

    # ---------- 主流程 ----------
    def run(self):
        try:
            self.log_signal.emit("正在初始化数据库...")
            time0 = time.time()

            if not init_database():
                self.log_signal.emit("❌ 数据库初始化失败")
                return

            # 1. 导入数据
            rows1 = import_excel_to_db(
                self.file1, self.sheet_name1, TEMP_TABLE1,
                is_file1=True, chunk_size=self.chunk_size
            )
            self.log_signal.emit(f"✅ 平台表导入完成，共 {rows1} 行")

            rows2 = import_excel_to_db(
                self.file2, self.sheet_name2, TEMP_TABLE2,
                is_file1=False, skip_rows=self.skip_rows, chunk_size=self.chunk_size
            )
            self.log_signal.emit(f"✅ ERP表导入完成，共 {rows2} 行")

            # 预先准备资产分类映射表数据
            mapping_prepared = prepare_asset_category_mapping(self.rules, self.rule_file)
            if mapping_prepared:
                self.log_signal.emit("✅ 资产分类映射表准备完成")

            # 2. 建索引
            create_compare_index(TEMP_TABLE1, ["_pk_concat"])
            create_compare_index(TEMP_TABLE2, ["_pk_concat"])

            # 3. 生成 _pk_concat
            expr1 = self._build_pk_expr("t1", is_file1=True)
            expr2 = self._build_pk_expr("t2", is_file1=False)
            self._add_concat_pk_column(TEMP_TABLE1, expr1)
            self._add_concat_pk_column(TEMP_TABLE2, expr2)

            # 4. 为表二添加计算字段
            self._add_calculated_fields(TEMP_TABLE2, is_file1=False)

            # 5. SQL 计算共同/缺失/多余
            diff_df = self._diff_by_mysql()
            common_str = diff_df.at[0, 'common_keys'] or ''
            missing_str = diff_df.at[0, 'missing_keys'] or ''
            extra_str = diff_df.at[0, 'extra_keys'] or ''

            common_codes = set(common_str.split('||')) if common_str else set()
            missing_in_file2 = set(missing_str.split('||')) if missing_str else set()
            missing_in_file1 = set(extra_str.split('||')) if extra_str else set()

            # 6. 拉取缺失/多余行
            if missing_in_file2:
                self.missing_rows = fetch_rows_by_pk(
                    TEMP_TABLE1, ["_pk_concat"], missing_in_file2
                ).to_dict(orient='records')
            if missing_in_file1:
                self.extra_in_file2 = fetch_rows_by_pk(
                    TEMP_TABLE2, ["_pk_concat"], missing_in_file1
                ).to_dict(orient='records')

            # 显示缺失和多余的主键信息
            if self.missing_rows:
                self.log_signal.emit(f"❌ 表一中有 {len(self.missing_rows)} 条数据在表二中缺失:")
                for i, row in enumerate(self.missing_rows[:5]):  # 只显示前5条
                    pk_values = [str(row.get(pk, '')) for pk in self.primary_keys]
                    pk_str = " + ".join(pk_values)
                    self.log_signal.emit(f"  {i + 1}. {pk_str}")
                if len(self.missing_rows) > 5:
                    self.log_signal.emit(f"  ... 还有 {len(self.missing_rows) - 5} 条缺失记录")

            if self.extra_in_file2:
                self.log_signal.emit(f"⚠️ 表二中有 {len(self.extra_in_file2)} 条数据在表一中不存在:")
                for i, row in enumerate(self.extra_in_file2[:5]):  # 只显示前5条
                    pk_values = [str(row.get(pk, '')) for pk in self.primary_keys]
                    pk_str = " + ".join(pk_values)
                    self.log_signal.emit(f"  {i + 1}. {pk_str}")
                if len(self.extra_in_file2) > 5:
                    self.log_signal.emit(f"  ... 还有 {len(self.extra_in_file2) - 5} 条多余记录")

            if not common_codes:
                self.log_signal.emit("警告：两个文件中没有共同的主键！")
                return

            # 7. 在数据库中进行字段差异比对
            diff_full_rows = self._compare_fields_in_db(common_codes)
            diff_count = len(diff_full_rows)

            # 8. 构建结果摘要
            equal_count = len(common_codes) - diff_count
            primary_key_str = " + ".join(self.primary_keys)

            self.diff_full_rows = diff_full_rows
            self.summary = {
                "primary_key": primary_key_str,
                "total_file1": rows1,
                "total_file2": rows2,
                "missing_count": len(missing_in_file2),
                "extra_count": len(missing_in_file1),
                "common_count": len(common_codes),
                "diff_count": diff_count,
                "equal_count": equal_count,
                "diff_ratio": diff_count / len(common_codes) if len(common_codes) > 0 else 0.0,
            }

            if diff_count == 0:
                self.log_signal.emit("✅【共同主键的数据完全一致】，没有差异。")
            else:
                self.log_signal.emit(f"❌【存在差异的记录】（共 {diff_count} 行）")
                # 显示具体的差异信息
                for i, diff_record in enumerate(diff_full_rows[:10]):  # 只显示前10条差异
                    src = diff_record["source"]
                    tgt = diff_record["target"]

                    # 显示主键信息
                    pk_values = [str(src.get(pk, '')) for pk in self.primary_keys]
                    pk_str = " + ".join(pk_values)
                    self.log_signal.emit(f"  {i + 1}. 主键: {pk_str}")

                    # 查找并显示具体差异的字段
                    for field_name, rule in self.rules.items():
                        if rule.get("is_primary"):
                            continue  # 跳过主键字段

                        src_value = src.get(field_name, "")
                        tgt_value = tgt.get(field_name, "")

                        # 获取目标表中的正确字段名
                        if rule.get("calc_rule") and rule.get("data_type") in ["数值", "文本"]:
                            tgt_value = tgt.get(field_name, "")
                        else:
                            table2_field = rule.get("table2_field", field_name)
                            tgt_value = tgt.get(table2_field, "") or tgt.get(field_name, "")

                        # 标准化值用于比较
                        norm_src = self.normalize_value(src_value)
                        norm_tgt = self.normalize_value(tgt_value)

                        # 对于日期字段，需要特殊处理格式
                        if rule.get("data_type") == "日期":
                            norm_src = self._normalize_date_format(norm_src)
                            norm_tgt = self._normalize_date_format(norm_tgt)

                        # 对于数值字段，考虑精度处理
                        elif rule.get("data_type") == "数值":
                            tail_diff = int(rule.get("tail_diff", 0))
                            try:
                                # 尝试转换为数值并按精度比较
                                src_num = float(norm_src) if norm_src else 0
                                tgt_num = float(norm_tgt) if norm_tgt else 0

                                # 如果设置了尾差，则按尾差精度比较
                                if tail_diff > 0:
                                    src_rounded = round(src_num, tail_diff)
                                    tgt_rounded = round(tgt_num, tail_diff)

                                    # 只有在超出尾差范围时才显示为差异
                                    if abs(src_rounded - tgt_rounded) > (10 ** (-tail_diff)):
                                        # 格式化显示值，保持精度一致性
                                        src_display = f"{src_num:.{tail_diff}f}" if norm_src else ""
                                        tgt_display = f"{tgt_num:.{tail_diff}f}" if norm_tgt else ""
                                        self.log_signal.emit(
                                            f"    - {field_name}: 表一='{src_display}' ≠ 表二='{tgt_display}'")
                                else:
                                    # 没有设置尾差时，直接比较数值
                                    if src_num != tgt_num:
                                        self.log_signal.emit(
                                            f"    - {field_name}: 表一='{src_value}' ≠ 表二='{tgt_value}'")
                            except (ValueError, TypeError):
                                # 如果不能转换为数值，按字符串比较
                                if norm_src != norm_tgt:
                                    self.log_signal.emit(f"    - {field_name}: 表一='{src_value}' ≠ 表二='{tgt_value}'")

                        # 对于文本字段，需要特殊处理标准化
                        elif rule.get("data_type") == "文本":
                            # 特殊处理资产分类字段
                            if field_name == "资产分类":
                                if mapping_prepared:
                                    # 获取映射表
                                    mapping_df = _load_asset_category_mapping(self.rule_file)
                                    if not mapping_df.empty and '同源目录完整名称' in mapping_df.columns and '同源目录编码' in mapping_df.columns:
                                        # 创建映射字典
                                        category_mapping = dict(zip(mapping_df['同源目录完整名称'].astype(str),
                                                                    mapping_df['同源目录编码'].astype(str)))

                                        # 获取表一的编码（通过映射）
                                        src_code = category_mapping.get(str(src_value), str(src_value))
                                        src_code_prefix = src_code[:2] if len(src_code) >= 2 else src_code

                                        # 获取表二的"资产明细类别"字段值（实际用于对比的字段）
                                        actual_tgt_value = tgt.get("资产明细类别", "")
                                        tgt_code_prefix = str(actual_tgt_value)[:2] if len(
                                            str(actual_tgt_value)) >= 2 else str(actual_tgt_value)

                                        # 比较前两位
                                        if src_code_prefix != tgt_code_prefix:
                                            # 显示原始中文信息而不是编码
                                            self.log_signal.emit(
                                                f"    - {field_name}: 表一='{src_value}' ≠ 表二='{actual_tgt_value}' (编码前两位不匹配: {src_code_prefix} vs {tgt_code_prefix})")
                                    else:
                                        # 映射表不可用时的回退处理
                                        norm_src_text = self._normalize_text_value(src_value)
                                        norm_tgt_text = self._normalize_text_value(tgt_value)
                                        if norm_src_text != norm_tgt_text:
                                            self.log_signal.emit(
                                                f"    - {field_name}: 表一='{src_value}' ≠ 表二='{tgt_value}'")
                                else:
                                    # 映射表未准备好的回退处理
                                    norm_src_text = self._normalize_text_value(src_value)
                                    norm_tgt_text = self._normalize_text_value(tgt_value)
                                    if norm_src_text != norm_tgt_text:
                                        self.log_signal.emit(
                                            f"    - {field_name}: 表一='{src_value}' ≠ 表二='{tgt_value}'")
                            # 特殊处理监管资产属性字段，只对比二级分类
                            elif field_name == "监管资产属性":
                                # 提取二级分类进行比较
                                src_second_level = self._extract_second_level(str(src_value))
                                tgt_second_level = self._extract_second_level(str(tgt_value))

                                if src_second_level != tgt_second_level:
                                    self.log_signal.emit(
                                        f"    - {field_name}: 表一='{src_value}' ≠ 表二='{tgt_value}' (二级分类不匹配: '{src_second_level}' vs '{tgt_second_level}')")
                            # 对折旧方法字段进行特殊处理
                            elif "折旧方法" in field_name:
                                norm_src_text = self._normalize_depreciation_method(src_value, is_file1=True)
                                norm_tgt_text = self._normalize_depreciation_method(tgt_value, is_file1=False)
                                # 比较标准化后的值
                                if norm_src_text != norm_tgt_text:
                                    self.log_signal.emit(f"    - {field_name}: 表一='{src_value}' ≠ 表二='{tgt_value}'")
                            else:
                                # 对其他文本值进行标准化处理
                                norm_src_text = self._normalize_text_value(src_value)
                                norm_tgt_text = self._normalize_text_value(tgt_value)
                                # 比较标准化后的值
                                if norm_src_text != norm_tgt_text:
                                    self.log_signal.emit(f"    - {field_name}: 表一='{src_value}' ≠ 表二='{tgt_value}'")
                        else:
                            # 其他类型按原逻辑比较
                            if norm_src != norm_tgt:
                                self.log_signal.emit(f"    - {field_name}: 表一='{src_value}' ≠ 表二='{tgt_value}'")
                if diff_count > 10:
                    self.log_signal.emit(f"  ... 还有 {diff_count - 10} 条差异记录未显示")

            time1 = time.time()
            self.log_signal.emit(f"✅ 对比完成，总耗时{time1 - time0:.1f}s")

        except Exception as e:
            logging.error(traceback.format_exc())
            self.log_signal.emit(f"❌ 发生错误：{str(e)}")
        finally:
            try:
                drop_tables()
            except:
                pass
            gc.collect()
            self.quit()
            self.wait()

    def _normalize_date_format(self, date_str):
        """
        标准化日期格式
        支持 '2019-12-19' 和 '20191219' 等格式
        """
        if not date_str:
            return ""

        # 移除所有非数字字符，获取纯数字
        digits = re.sub(r'\D', '', date_str)

        # 如果是8位数字，假设为 YYYYMMDD 格式
        if len(digits) == 8:
            try:
                return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
            except:
                return date_str  # 如果转换失败，返回原始值

        # 如果已经包含连字符，尝试标准化
        if '-' in date_str:
            try:
                parts = date_str.split('-')
                if len(parts) == 3:
                    year, month, day = parts
                    return f"{year}-{int(month):02d}-{int(day):02d}"
            except:
                return date_str  # 如果转换失败，返回原始值

        # 其他情况返回原始值
        return date_str

