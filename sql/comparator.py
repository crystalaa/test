# comparator.py
import sys
import time
import traceback
import logging
import pandas as pd
import re
import gc
from concurrent.futures import ThreadPoolExecutor
from PyQt5.QtCore import QThread, pyqtSignal
from data_handler import read_excel_fast, read_mapping_table
from rule_handler import read_enum_mapping, read_erp_combo_map
from db_handler import init_database, import_excel_to_db, execute_query, drop_tables

# 临时表名
TEMP_TABLE1 = 'temp_table1'
TEMP_TABLE2 = 'temp_table2'


class CompareWorker(QThread):
    """用于在独立线程中执行比较操作"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)  # 用于更新进度条

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
        self.chunk_size = chunk_size  # 减小分块大小以节省内存

        # 结果存储
        self.missing_assets = []
        self.diff_records = []
        self.summary = {}
        self.missing_rows = []
        self.extra_in_file2 = []
        self.diff_full_rows = []
        self.enum_map = read_enum_mapping(rule_file)
        self.erp_combo_map = read_erp_combo_map(rule_file)
        self.asset_code_to_original = {}  # 资产分类编码到原始值的映射

    @staticmethod
    def normalize_value(val):
        """统一空值表示"""
        if pd.isna(val) or val is None or (isinstance(val, str) and str(val).strip() == ''):
            return ''
        return str(val).strip()

    def calculate_field(self, df, calc_rule, data_type):
        """根据计算规则和数据类型生成ERP表字段值"""
        if not calc_rule:
            return None

        try:
            # 处理字符串截取
            if '[:' in calc_rule and ']' in calc_rule:
                field, length_str = calc_rule.split('[:')
                field = field.strip()
                length = int(length_str.strip(']').strip())
                if field not in df.columns:
                    raise Exception(f"字段不存在：{field}")
                return df[field].fillna('').astype(str).str[:length]

            # 根据数据类型处理不同运算
            if data_type == "文本":
                fields = [f.strip() for f in calc_rule.split('+')]
                missing_fields = [f for f in fields if f not in df.columns]
                if missing_fields:
                    raise Exception(f"表达式中包含不存在的字段：{missing_fields}")

                result = df[fields[0]].fillna('').astype(str)
                for field in fields[1:]:
                    result += df[field].fillna('').astype(str)
                return result

            elif data_type == "数值":
                import re
                field_pattern = re.compile(r'[a-zA-Z\u4e00-\u9fa5]+')
                fields_in_rule = field_pattern.findall(calc_rule)
                missing_fields = [f for f in fields_in_rule if f not in df.columns]
                if missing_fields:
                    raise Exception(f"表达式中包含不存在的字段：{missing_fields}")

                # 转换为数值类型，空值处理为0
                df_numeric = df.copy()
                for field in fields_in_rule:
                    if "折旧" in field:
                        df_numeric[field] = pd.to_numeric(df[field], errors='coerce').fillna(0).abs()
                    else:
                        df_numeric[field] = pd.to_numeric(df[field], errors='coerce').fillna(0)

                result = df_numeric.eval(calc_rule)
                return result
            else:
                raise Exception(f"不支持的数据类型：{data_type}，请指定为'文本'或'数值'")

        except Exception as e:
            raise Exception(f"计算规则执行失败（{calc_rule}）：{str(e)}")

    def _get_value(self, df, part):
        """辅助函数：解析运算中的值（可能是字段或数值）"""
        part = part.strip()
        if part in df.columns:
            return pd.to_numeric(df[part], errors='coerce').fillna(0)
        try:
            return float(part)
        except ValueError:
            raise Exception(f"无法解析值：{part}（不是字段也不是数值）")

    def values_equal_by_rule(self, val1, val2, data_type, tail_diff, field_name=""):
        """根据规则判断两个值是否相等"""
        val1 = self.normalize_value(val1)
        val2 = self.normalize_value(val2)

        if val1 == "" and val2 == "":
            return True

        # 监管资产属性字段特殊处理
        if field_name == "监管资产属性":
            def extract_last_segment(val, sep):
                if not val:
                    return ""
                parts = str(val).split(sep)
                return parts[-1].strip()

            val1_clean = extract_last_segment(val1, '\\')
            val2_clean = extract_last_segment(val2, '-')
            return val1_clean == val2_clean

        # 关联实物管理系统代码处理
        if field_name == "关联实物管理系统代码":
            plat = val1.strip()
            erp_combo = val2.strip()
            allowed_combos = self.erp_combo_map.get(plat, [])
            return erp_combo in allowed_combos

        # 线站电压等级处理
        if field_name == "线站电压等级":
            code1 = self.enum_map.get(val1, val1)
            return code1 == val2

        # 布尔值映射逻辑
        bool_map = {"是": "Y", "否": "N", "Y": "是", "N": "否"}
        if val1 in bool_map and val2 in bool_map:
            if bool_map[val1] == val2 or val1 == bool_map[val2]:
                return True

        # 折旧方法处理
        if field_name == "折旧方法":
            if (val1 == "年限平均法" and val2 == "直线法") or \
                    (val1 == "直线法" and val2 == "年限平均法"):
                return True

        if field_name == "资产分类":
            if val1.isdigit():
                val1 = val1[:2]
            if val2.isdigit():
                val2 = val2[:2]
            return val1 == val2

        # 数值型比较
        if data_type == "数值":
            num1 = pd.to_numeric(val1, errors='coerce')
            num2 = pd.to_numeric(val2, errors='coerce')

            if pd.isna(num1) and pd.isna(num2):
                return True
            elif pd.isna(num1) or pd.isna(num2):
                return False

            if "折旧" in field_name:
                num1 = abs(num1)
                num2 = abs(num2)

            if tail_diff is None:
                return num1 == num2
            else:
                return abs(num1 - num2) <= float(tail_diff)

        # 日期型比较
        elif data_type == "日期":
            def parse_date(date_str):
                if pd.isna(date_str) or str(date_str).strip() == "":
                    return ""
                date_str = str(date_str).strip()
                formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y年%m月%d日',
                           '%m-%d-%Y', '%m/%d/%Y', '%Y%m%d', '%Y-%m-%d %H:%M:%S']
                for fmt in formats:
                    try:
                        return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
                    except (ValueError, TypeError):
                        continue
                return date_str

            parsed1 = parse_date(val1)
            parsed2 = parse_date(val2)

            if parsed1 == "" and parsed2 == "":
                return True

            if tail_diff == "月":
                cmp1 = parsed1[:7]
                cmp2 = parsed2[:7]
            elif tail_diff == "年":
                cmp1 = parsed1[:4]
                cmp2 = parsed2[:4]
            elif tail_diff == "日":
                cmp1 = parsed1[:10]
                cmp2 = parsed2[:10]  # 取年月日（YYYY-MM-DD）
            elif tail_diff == "时":
                cmp1 = parsed1[:13]
                cmp2 = parsed2[:13]  # 取年月日时（YYYY-MM-DD HH）
            elif tail_diff == "分":
                cmp1 = parsed1[:16]
                cmp2 = parsed2[:16]  # 取年月日时分（YYYY-MM-DD HH:MM）
            elif tail_diff == "秒":
                cmp1 = parsed1[:19]
                cmp2 = parsed2[:19]  # 取完整时间（YYYY-MM-DD HH:MM:SS）
            return cmp1 == cmp2

        # 文本型比较
        elif data_type == "文本":
            return val1 == val2

        return val1 == val2

    def convert_asset_category(self, df1, mapping_df):
        """资产分类转换逻辑 - 使用merge优化"""
        asset_category_col1 = "资产分类"
        # asset_category_col2 = "SAP资产类别描述"

        source_col = "同源目录完整名称"
        # target_col = "21年资产目录大类"
        # detail_col = "ERP资产明细类描述"
        code_col = "同源目录编码"
        # erp_detail_col = "ERP资产明细类别"

        # 为平台表创建映射
        # 先处理一对一映射
        unique_mapping = mapping_df.drop_duplicates(subset=[source_col], keep='first')
        if not unique_mapping.empty:
            unique_map = dict(zip(unique_mapping[source_col], unique_mapping[code_col]))
            df1[asset_category_col1] = df1[asset_category_col1].map(
                lambda x: unique_map.get(x, x) if pd.notna(x) else x
            )
            # 更新asset_code_to_original
            for k, v in unique_map.items():
                self.asset_code_to_original[str(v)] = k

        return df1

    def _process_batch_comparison(self, df1_batch, df2_batch, batch_index, total_batches, df1_original, df2_original,
                                  pk_mapping):
        """处理单个批次的数据比较"""
        try:
            self.log_signal.emit(f"正在处理第 {batch_index + 1}/{total_batches} 批数据...")

            # 格式化索引
            df1_batch.index = df1_batch.index.map(lambda x: ' + '.join(x) if isinstance(x, tuple) else str(x))
            df2_batch.index = df2_batch.index.map(lambda x: ' + '.join(x) if isinstance(x, tuple) else str(x))

            batch_diff_dict = {}
            batch_diff_full_rows = []

            for field1, rule in self.rules.items():
                # 只比对规则文件中定义的列
                if field1 not in df1_batch.columns or field1 not in df2_batch.columns:
                    continue

                data_type = rule["data_type"]
                tail_diff = rule.get("tail_diff")

                # 向量化获取两列数据
                series1 = df1_batch[field1]
                if field1 == "资产分类":
                    series2 = df2_batch['原21版资产分类']
                    default_series2 = df2_batch[field1]
                else:
                    series2 = df2_batch[field1]

                if data_type == "数值":
                    # 数值型比较
                    series1_num = pd.to_numeric(series1, errors='coerce')
                    series2_num = pd.to_numeric(series2, errors='coerce')
                    # 如果字段名包含"折旧"，取绝对值
                    if "折旧" in field1:
                        series1_num = series1_num.abs()
                        series2_num = series2_num.abs()
                    if tail_diff is None:
                        diff_mask = (series1_num != series2_num) & \
                                    ~(pd.isna(series1_num) & pd.isna(series2_num))
                    else:
                        diff_mask = (abs(series1_num - series2_num) > float(tail_diff)) & \
                                    ~(pd.isna(series1_num) & pd.isna(series2_num))

                elif data_type == "日期":
                    # 日期型比较
                    # 日期型比较：先统一解析为标准日期格式
                    def parse_date(date_str):
                        """尝试多种格式解析日期，返回标准化字符串（YYYY-MM-DD）"""
                        if pd.isna(date_str):
                            return ""
                        date_str = str(date_str).strip()
                        if not date_str:
                            return ""
                        # 尝试多种常见日期格式
                        formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y年%m月%d日',
                                   '%m-%d-%Y', '%m/%d/%Y', '%Y%m%d', '%Y-%m-%d %H:%M:%S']
                        for fmt in formats:
                            try:
                                return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
                            except (ValueError, TypeError):
                                continue
                        # 如果所有格式都解析失败，返回原始字符串
                        return date_str

                    # 统一解析两列日期
                    series1_parsed = series1.apply(parse_date)
                    series2_parsed = series2.apply(parse_date)

                    # 处理空值情况
                    both_empty = (series1_parsed == "") & (series2_parsed == "")

                    # 根据精度需求截取
                    if tail_diff == "月":
                        series1_cmp = series1_parsed.str[:7]  # YYYY-MM
                        series2_cmp = series2_parsed.str[:7]
                    elif tail_diff == "年":
                        series1_cmp = series1_parsed.str[:4]  # YYYY
                        series2_cmp = series2_parsed.str[:4]
                    elif tail_diff == "日":
                        series1_cmp = series1_parsed.str[:10]
                        series2_cmp = series2_parsed.str[:10]  # 取年月日（YYYY-MM-DD）
                    elif tail_diff == "时":
                        series1_cmp = series1_parsed.str[:13]
                        series2_cmp = series2_parsed.str[:13]  # 取年月日时（YYYY-MM-DD HH）
                    elif tail_diff == "分":
                        series1_cmp = series1_parsed.str[:16]
                        series2_cmp = series2_parsed.str[:16]  # 取年月日时分（YYYY-MM-DD HH:MM）
                    elif tail_diff == "秒":
                        series1_cmp = series1_parsed.str[:19]
                        series2_cmp = series2_parsed.str[:19]

                    diff_mask = (series1_cmp != series2_cmp) & ~both_empty

                elif data_type == "文本":
                    def mapped_equal(a, b, field):
                        return self.values_equal_by_rule(a, b, "文本", None, field)

                    diff_mask = ~pd.Series([
                        mapped_equal(self.normalize_value(s1).strip(), self.normalize_value(s2).strip(), field1)
                        for s1, s2 in zip(series1, series2)
                    ], index=series1.index)

                # 找出有差异的行索引
                diff_indices = df1_batch[diff_mask].index

                # 批量添加差异记录
                for idx in diff_indices:
                    if idx not in batch_diff_dict:
                        batch_diff_dict[idx] = []

                    val1 = CompareWorker.normalize_value(series1.loc[idx])
                    val2 = CompareWorker.normalize_value(series2.loc[idx])
                    if field1 == "资产分类":
                        val2 = self.normalize_value(default_series2.loc[idx])
                    batch_diff_dict[idx].append((field1, val1, val2))

            # 为当前批次生成完整行数据
            for code_str, diffs in batch_diff_dict.items():
                try:
                    # 获取原始主键值
                    original_pk_values = pk_mapping.get(code_str, code_str)

                    # 构建筛选条件
                    if isinstance(original_pk_values, tuple):
                        # 多主键情况
                        condition1 = True
                        condition2 = True
                        for i, pk in enumerate(self.primary_keys):
                            condition1 = condition1 & (df1_original[pk].astype(str) == original_pk_values[i])
                            condition2 = condition2 & (df2_original[pk].astype(str) == original_pk_values[i])
                    else:
                        # 单主键情况
                        pk = self.primary_keys[0]
                        condition1 = (df1_original[pk].astype(str) == original_pk_values)
                        condition2 = (df2_original[pk].astype(str) == original_pk_values)

                    # 获取完整行数据
                    source_dict = df1_original[condition1].iloc[0].to_dict()
                    target_dict = df2_original[condition2].iloc[0].to_dict()

                    batch_diff_full_rows.append({
                        "source": source_dict,
                        "target": target_dict
                    })
                except (IndexError, KeyError, Exception):
                    # 出现异常时使用原来的方法作为备选
                    try:
                        source_dict = df1_batch.loc[code_str].to_dict()
                        target_dict = df2_batch.loc[code_str].to_dict()

                        # 手动添加主键信息
                        original_pk_values = pk_mapping.get(code_str, code_str)
                        if isinstance(original_pk_values, tuple):
                            for i, pk in enumerate(self.primary_keys):
                                source_dict[pk] = original_pk_values[i]
                                target_dict[pk] = original_pk_values[i]
                        else:
                            if self.primary_keys:
                                source_dict[self.primary_keys[0]] = original_pk_values
                                target_dict[self.primary_keys[0]] = original_pk_values

                        batch_diff_full_rows.append({
                            "source": source_dict,
                            "target": target_dict
                        })
                    except Exception:
                        pass  # 忽略无法处理的记录

            return batch_diff_dict, batch_diff_full_rows

        except Exception as e:
            self.log_signal.emit(f"批处理比较出错: {str(e)}")
            return {}, []

    def run(self):
        try:
            self.log_signal.emit("正在初始化数据库...")
            time0 = time.time()

            # 初始化数据库
            if not init_database():
                self.log_signal.emit("❌ 数据库初始化失败")
                return

            # 导入Excel文件到数据库
            self.log_signal.emit("正在导入平台表到数据库...")
            rows1 = import_excel_to_db(
                self.file1,
                self.sheet_name1,
                TEMP_TABLE1,
                is_file1=True,
                chunk_size=self.chunk_size
            )
            self.log_signal.emit(f"✅ 平台表导入完成，共 {rows1} 行数据")

            self.log_signal.emit("正在导入ERP表到数据库...")
            rows2 = import_excel_to_db(
                self.file2,
                self.sheet_name2,
                TEMP_TABLE2,
                is_file1=False,
                skip_rows=self.skip_rows,
                chunk_size=self.chunk_size
            )
            self.log_signal.emit(f"✅ ERP表导入完成，共 {rows2} 行数据")

            # 读取数据进行处理
            self.log_signal.emit("正在从数据库读取数据进行处理...")

            # 读取平台表数据
            df1 = execute_query(f"SELECT * FROM {TEMP_TABLE1}")
            # 删除自增id列
            if 'id' in df1.columns:
                df1 = df1.drop(columns=['id'])
            self.log_signal.emit(f"✅ 平台表读取完成，共 {len(df1)} 行数据")

            # 读取ERP表数据
            df2 = execute_query(f"SELECT * FROM {TEMP_TABLE2}")
            # 删除自增id列
            if 'id' in df2.columns:
                df2 = df2.drop(columns=['id'])
            self.log_signal.emit(f"✅ ERP表读取完成，共 {len(df2)} 行数据")

            self.log_signal.emit("开始比较数据...")
            # 读取资产分类映射表
            mapping_df = read_mapping_table(self.rule_file)
            # 转换资产分类
            df1 = self.convert_asset_category(df1, mapping_df)

            # 检查数据行是否存在
            if df1.empty:
                self.log_signal.emit("❌ 错误：平台表除了表头外没有数据行，请检查文件内容！")
                return

            if df2.empty:
                self.log_signal.emit("❌ 错误：ERP表除了表头外没有数据行，请检查文件内容！")
                return

            # 清理列名
            df1.columns = df1.columns.str.replace('[*\\s]', '', regex=True)
            df2.columns = df2.columns.str.replace('[*\\s]', '', regex=True)

            # 检查规则中的列是否存在
            table2_columns_to_check = []
            for rule in self.rules.values():
                if not rule.get("calc_rule") and rule["table2_field"]:
                    table2_columns_to_check.append(rule["table2_field"])

            table1_columns_to_compare = list(self.rules.keys())
            missing_in_file1 = [col for col in table1_columns_to_compare if col not in df1.columns]
            missing_in_file2 = [col for col in table2_columns_to_check if col not in df2.columns]

            if missing_in_file1 or missing_in_file2:
                error_msg = ""
                if missing_in_file1:
                    error_msg += f"平台表缺失以下规则定义的列：{', '.join(missing_in_file1)}\n"
                if missing_in_file2:
                    error_msg += f"ERP表缺失以下规则定义的列：{', '.join(missing_in_file2)}\n"
                self.log_signal.emit(f"❌ 比对失败：{error_msg}")
                return

            # 处理计算字段
            self.log_signal.emit("✅ 开始处理计算字段...")
            calc_temp_fields = {}

            for field1, rule in self.rules.items():
                if rule.get("calc_rule") and field1 in df2.columns:
                    df2.drop(columns=[field1], inplace=True)
                    self.log_signal.emit(f"忽略ERP表中原有的 '{rule['table2_field']}' 列，将使用计算规则生成的新列")
                if field1 != rule["table2_field"] and field1 in df2.columns:
                    df2.drop(columns=[field1], inplace=True)

                if rule.get("calc_rule"):
                    self.log_signal.emit(f"正在计算ERP表字段: {field1} (规则: {rule['calc_rule']})")
                    try:
                        temp_field = f"__calc_{field1}__"
                        calc_temp_fields[field1] = temp_field
                        df2[temp_field] = self.calculate_field(
                            df2,
                            rule["calc_rule"],
                            rule["data_type"]
                        )
                    except Exception as e:
                        self.log_signal.emit(f"⚠️ 计算字段 {field1} 时出错: {str(e)}")

            # 字段映射
            mapped_columns = {}
            for field1, rule in self.rules.items():
                field2 = rule["table2_field"]
                if field1 in calc_temp_fields:
                    mapped_columns[calc_temp_fields[field1]] = field1
                elif field2 in df2.columns:
                    mapped_columns[field2] = field1

            mapped_log = "\n".join([f"  {k} -> {v}" for k, v in mapped_columns.items()])
            self.log_signal.emit(f"字段映射关系：\n{mapped_log}")
            df2.rename(columns=mapped_columns, inplace=True)

            # 删除临时字段释放内存
            for temp_field in calc_temp_fields.values():
                if temp_field in df2.columns:
                    del df2[temp_field]
            del calc_temp_fields
            gc.collect()

            # 只保留需要比对的列，减少内存占用
            all_needed_columns = list(set(table1_columns_to_compare + self.primary_keys))
            df1 = df1[all_needed_columns].copy()
            df2 = df2[all_needed_columns].copy()
            gc.collect()

            # 主键检查
            if not self.primary_keys:
                self.log_signal.emit("❌ 错误：规则文件中未定义主键字段，请检查规则文件！")
                return

            # 检查主键列在数据中是否存在
            for pk in self.primary_keys:
                if pk not in df1.columns:
                    self.log_signal.emit(f"❌ 错误：平台表中不存在主键列 '{pk}'")
                    return
                if pk not in df2.columns:
                    self.log_signal.emit(f"❌ 错误：ERP表中不存在主键列 '{pk}'")
                    return

            # 检查主键是否有重复值
            df1_duplicates = df1[df1.duplicated(subset=self.primary_keys, keep=False)]
            if not df1_duplicates.empty:
                duplicate_count = df1_duplicates.shape[0]
                self.log_signal.emit(f"❌ 错误：平台表中存在 {duplicate_count} 条重复的主键记录")
                # 显示前几个重复的主键示例
                duplicate_examples = df1_duplicates[self.primary_keys].head(5)
                example_lines = []
                for _, row in duplicate_examples.iterrows():
                    keys = [str(row[pk]) for pk in self.primary_keys]
                    example_lines.append(" + ".join(keys))
                examples = "\n".join([f" - {example}" for example in example_lines])
                self.log_signal.emit(f"重复主键示例（前5个）：\n{examples}")
                return

            df2_duplicates = df2[df2.duplicated(subset=self.primary_keys, keep=False)]
            if not df2_duplicates.empty:
                duplicate_count = df2_duplicates.shape[0]
                self.log_signal.emit(f"❌ 错误：ERP表中存在 {duplicate_count} 条重复的主键记录")
                # 显示前几个重复的主键示例
                duplicate_examples = df2_duplicates[self.primary_keys].head(5)
                example_lines = []
                for _, row in duplicate_examples.iterrows():
                    keys = [str(row[pk]) for pk in self.primary_keys]
                    example_lines.append(" + ".join(keys))
                examples = "\n".join([f" - {example}" for example in example_lines])
                self.log_signal.emit(f"重复主键示例（前5个）：\n{examples}")
                return

            # 检查主键列是否有空值
            for pk in self.primary_keys:
                df1_empty_keys = df1[pd.isna(df1[pk]) | (df1[pk].astype(str).astype(str).str.strip() == '')]
                df2_empty_keys = df2[pd.isna(df2[pk]) | (df2[pk].astype(str).astype(str).str.strip() == '')]

                if len(df1_empty_keys) > 0:
                    self.log_signal.emit(f"⚠️ 警告：平台表中主键列 '{pk}' 存在 {len(df1_empty_keys)} 条空值记录")

                if len(df2_empty_keys) > 0:
                    self.log_signal.emit(f"⚠️ 警告：ERP表中主键列 '{pk}' 存在 {len(df2_empty_keys)} 条空值记录")

            # 保存原始数据帧用于导出（包含主键列）
            df1_original = df1.copy()
            df2_original = df2.copy()

            # 设置主键索引
            df1.set_index(self.primary_keys, inplace=True)
            df2.set_index(self.primary_keys, inplace=True)

            # 规范化索引格式
            df1.index = df1.index.map(lambda x: tuple(str(i) for i in x) if isinstance(x, tuple) else (str(x),))
            df2.index = df2.index.map(lambda x: tuple(str(i) for i in x) if isinstance(x, tuple) else (str(x),))

            # 检查索引中是否有空值
            df1_empty_index = df1.index.map(
                lambda x: any(pd.isna(i) or str(i).strip() == '' for i in (x if isinstance(x, tuple) else (x,))))
            df2_empty_index = df2.index.map(
                lambda x: any(pd.isna(i) or str(i).strip() == '' for i in (x if isinstance(x, tuple) else (x,))))

            df1_empty_count = sum(df1_empty_index)
            df2_empty_count = sum(df2_empty_index)

            if df1_empty_count > 0:
                self.log_signal.emit(f"⚠️ 警告：平台表中有 {df1_empty_count} 条记录的主键为空")

            if df2_empty_count > 0:
                self.log_signal.emit(f"⚠️ 警告：ERP表中有 {df2_empty_count} 条记录的主键为空")

            if len(df1) != len(df2):
                self.log_signal.emit(f"提示：两个文件的行数不一致（平台表有 {len(df1)} 行，ERP表有 {len(df2)} 行）")

            # 查找ERP表中缺失的主键
            missing_in_file2 = df1.index.difference(df2.index)
            if not missing_in_file2.empty:
                missing_df = df1.loc[missing_in_file2].copy()
                original_codes = missing_in_file2.map(lambda x: ' + '.join(map(str, x)))
                missing_df.reset_index(drop=True, inplace=True)

                for idx, key in enumerate(self.primary_keys):
                    missing_df.insert(1 + idx, key, original_codes.map(lambda x: x.split(' + ')[idx]))

                self.missing_rows = missing_df.to_dict(orient='records')
                missing_list = "\n".join([f" - {code}" for code in missing_in_file2])
                self.log_signal.emit(f"【ERP表中缺失的主键】（共 {len(missing_in_file2)} 条）：\n{missing_list}")

            # 查找ERP表中多出的主键
            missing_in_file1 = df2.index.difference(df1.index)
            if not missing_in_file1.empty:
                missing_df_file1 = df2.loc[missing_in_file1].copy()
                original_codes_file1 = missing_in_file1.map(lambda x: ' + '.join(map(str, x)))
                missing_df_file1.reset_index(drop=True, inplace=True)

                for idx, key in enumerate(self.primary_keys):
                    missing_df_file1.insert(1 + idx, key, original_codes_file1.map(lambda x: x.split(' + ')[idx]))

                self.extra_in_file2 = missing_df_file1.to_dict(orient='records')
                missing_list_file1 = "\n".join([f" - {code}" for code in missing_in_file1])
                self.log_signal.emit(
                    f"【ERP表中多出的主键】（平台表中没有，共 {len(missing_in_file1)} 条）：\n{missing_list_file1}")

            # 找出共同的主键
            common_codes = df1.index.intersection(df2.index)
            if common_codes.empty:
                self.log_signal.emit("警告：两个文件中没有共同的主键！")
                return

            # 创建主键到原始值的映射，用于恢复导出数据中的主键列
            pk_mapping = {}
            for code in common_codes:
                code_str = ' + '.join(code) if isinstance(code, tuple) else str(code)
                pk_mapping[code_str] = code

            # 分批处理数据比较以节省内存
            try:
                self.log_signal.emit("开始进行分批向量化数据比较...")

                # 将common_codes分批处理
                BATCH_SIZE = 10000  # 每批处理5000条记录
                common_codes_list = list(common_codes)
                total_records = len(common_codes_list)
                total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE

                self.log_signal.emit(f"共 {total_records} 条共同记录，将分 {total_batches} 批处理")

                # 用于存储所有差异
                all_diff_dict = {}
                all_diff_full_rows = []

                for batch_idx in range(total_batches):
                    start_idx = batch_idx * BATCH_SIZE
                    end_idx = min((batch_idx + 1) * BATCH_SIZE, total_records)
                    batch_codes = common_codes_list[start_idx:end_idx]

                    # 获取当前批次的数据
                    df1_batch = df1.loc[batch_codes].copy()
                    df2_batch = df2.loc[batch_codes].copy()

                    # 处理当前批次
                    batch_diff_dict, batch_diff_full_rows = self._process_batch_comparison(
                        df1_batch, df2_batch, batch_idx, total_batches, df1_original, df2_original, pk_mapping)

                    # 合并到总差异字典
                    all_diff_dict.update(batch_diff_dict)
                    all_diff_full_rows.extend(batch_diff_full_rows)

                    # 清理当前批次数据以释放内存
                    del df1_batch, df2_batch, batch_diff_dict, batch_diff_full_rows
                    gc.collect()

                    # 每处理10批报告一次进度
                    if (batch_idx + 1) % 10 == 0 or batch_idx == total_batches - 1:
                        self.log_signal.emit(
                            f"已完成 {batch_idx + 1}/{total_batches} 批处理，当前发现 {len(all_diff_dict)} 条差异记录")

                diff_dict = all_diff_dict
                self.diff_full_rows = all_diff_full_rows
                self.log_signal.emit(f"分批向量化比较完成，共发现 {len(diff_dict)} 条差异记录")

            except Exception as e:
                self.log_signal.emit(f"分批向量化比较出错: {str(e)}")
                raise e

            # 生成日志和汇总信息
            diff_log_messages = []

            # 为了节省内存，我们只处理前10000条差异记录的详细信息
            max_detail_records = 10000
            processed_count = 0

            for code, diffs in diff_dict.items():
                if processed_count >= max_detail_records:
                    # 超过最大详细记录数，只统计不生成详细日志
                    remaining_count = len(diff_dict) - processed_count
                    if remaining_count > 0:
                        diff_log_messages.append(f"\n...还有 {remaining_count} 条差异记录未显示...")
                    break

                code_str = ' + '.join(code) if isinstance(code, tuple) else str(code)
                diff_details = []

                for col, val1, val2 in diffs:
                    if col == "资产分类" and hasattr(self, 'asset_code_to_original'):
                        # 使用原始中文值显示
                        original_val1 = self.asset_code_to_original.get(val1, val1)
                        original_val2 = self.asset_code_to_original.get(val2, val2)
                        diff_details.append(f" - 列 [{col}] 不一致：平台表={original_val1}, ERP表={original_val2}")
                    else:
                        diff_details.append(f" - 列 [{col}] 不一致：平台表={val1}, ERP表={val2}")

                diff_log_messages.append(f"\n主键：{code}")
                diff_log_messages.extend(diff_details)
                processed_count += 1

            diff_count = len(diff_dict)
            equal_count = len(common_codes) - diff_count
            primary_key_str = " + ".join(self.primary_keys)

            self.summary = {
                "primary_key": primary_key_str,
                "total_file1": len(df1),
                "total_file2": len(df2),
                "missing_count": len(missing_in_file2),
                "extra_count": len(missing_in_file1),
                "common_count": len(common_codes),
                "diff_count": diff_count,
                "equal_count": equal_count,
                "diff_ratio": diff_count / len(common_codes) if len(common_codes) > 0 else 0.0,
            }
            self.asset_code_map = self.asset_code_to_original  # 仅多一行

            if diff_count == 0:
                self.log_signal.emit("【共同主键的数据完全一致】，没有差异。")
            else:
                self.log_signal.emit(f"【存在差异的列】（共 {diff_count} 行）：")
                if diff_log_messages:
                    # 限制日志输出长度以避免界面卡顿
                    if len(diff_log_messages) > 5000:
                        truncated_messages = diff_log_messages[:5000]
                        truncated_messages.append(f"\n...（还有 {len(diff_dict) - 5000} 条消息未显示）")
                        self.log_signal.emit('\n'.join(truncated_messages))
                    else:
                        self.log_signal.emit('\n'.join(diff_log_messages))
                else:
                    self.log_signal.emit("⚠️ 未找到具体差异列，请检查数据是否一致。")

            time1 = time.time()
            self.log_signal.emit(f"对比完成，总耗时{time1 - time0:.1f}s")

        except Exception as e:
            logging.error(traceback.format_exc())
            self.log_signal.emit(f"发生错误：{str(e)}")
        finally:
            # 清理临时表
            try:
                drop_tables()
            except:
                pass

            # 强制垃圾回收释放内存
            gc.collect()
            self.quit()
            self.wait()
