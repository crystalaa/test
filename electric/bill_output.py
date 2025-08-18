from decimal import Decimal

import pandas as pd
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
import os
import threading
from datetime import datetime


class ExcelProcessorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Excel数据处理工具")
        self.root.geometry("800x600")
        self.root.resizable(True, True)

        # 设置中文字体
        self.style = ttk.Style()
        self.style.configure("TLabel", font=("SimHei", 10))
        self.style.configure("TButton", font=("SimHei", 10))
        self.style.configure("TProgressbar", thickness=20)

        # 文件路径变量
        self.file1_path = tk.StringVar()
        self.file2_path = tk.StringVar()

        # 创建界面组件
        self.create_widgets()

    def create_widgets(self):
        # 标题
        title_label = ttk.Label(
            self.root,
            text="Excel数据处理工具",
            font=("SimHei", 16, "bold")
        )
        title_label.pack(pady=20)

        # 文件选择区域
        file_frame = ttk.Frame(self.root)
        file_frame.pack(fill=tk.X, padx=50, pady=10)

        # 原始表1选择
        ttk.Label(file_frame, text="原始表1:").grid(row=0, column=0, sticky=tk.W, pady=5)
        ttk.Entry(file_frame, textvariable=self.file1_path, width=50).grid(row=0, column=1, pady=5)
        ttk.Button(
            file_frame,
            text="浏览...",
            command=lambda: self.browse_file(self.file1_path, "选择原始表1")
        ).grid(row=0, column=2, padx=10, pady=5)

        # 原始表2选择
        ttk.Label(file_frame, text="原始表2:").grid(row=1, column=0, sticky=tk.W, pady=5)
        ttk.Entry(file_frame, textvariable=self.file2_path, width=50).grid(row=1, column=1, pady=5)
        ttk.Button(
            file_frame,
            text="浏览...",
            command=lambda: self.browse_file(self.file2_path, "选择原始表2")
        ).grid(row=1, column=2, padx=10, pady=5)

        # 处理按钮
        process_btn = ttk.Button(
            self.root,
            text="开始处理",
            command=self.start_processing,
            style="TButton"
        )
        process_btn.pack(pady=20)

        # 进度条
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            self.root,
            variable=self.progress_var,
            maximum=100
        )
        self.progress_bar.pack(fill=tk.X, padx=50, pady=10)

        # 状态信息
        self.status_var = tk.StringVar(value="请选择文件并点击开始处理")
        status_label = ttk.Label(
            self.root,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W
        )
        status_label.pack(side=tk.BOTTOM, fill=tk.X)

        # 日志区域
        log_frame = ttk.LabelFrame(self.root, text="处理日志")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=50, pady=10)

        self.log_text = tk.Text(log_frame, height=8, wrap=tk.WORD)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=scrollbar.set)
        self.log_text.config(state=tk.DISABLED)

    def browse_file(self, path_var, title):
        """打开文件选择对话框并更新路径变量"""
        file_path = filedialog.askopenfilename(
            title=title,
            filetypes=[("Excel files", "*.xlsx")]
        )
        if file_path:
            path_var.set(file_path)
            self.log(f"已选择: {os.path.basename(file_path)}")

    def log(self, message):
        """向日志区域添加信息"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        self.status_var.set(message)

    def update_progress(self, value):
        """更新进度条"""
        self.progress_var.set(value)
        self.root.update_idletasks()

    def start_processing(self):
        """检查文件并开始处理（在新线程中运行以避免界面冻结）"""
        file1 = self.file1_path.get()
        file2 = self.file2_path.get()

        if not file1 or not file2:
            messagebox.showerror("错误", "请选择原始表1和原始表2")
            return

        if not os.path.exists(file1):
            messagebox.showerror("错误", f"原始表1不存在: {file1}")
            return

        if not os.path.exists(file2):
            messagebox.showerror("错误", f"原始表2不存在: {file2}")
            return

        # 清空日志
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)

        # 禁用处理按钮
        for widget in self.root.winfo_children():
            if isinstance(widget, ttk.Button) and widget["text"] == "开始处理":
                widget.config(state=tk.DISABLED)
                break

        # 在新线程中处理数据，避免界面冻结
        threading.Thread(target=self.process_data, args=(file1, file2), daemon=True).start()

    def process_data(self, file1_path, file2_path):
        """处理数据的核心函数"""
        try:
            self.update_progress(10)
            self.log("开始读取文件...")

            # 读取数据
            df1 = pd.read_excel(file1_path, dtype={'借方发生额': 'object', '贷方发生额': 'object'})
            self.update_progress(25)
            self.log(f"已读取原始表1，共 {len(df1)} 行数据")

            df2 = pd.read_excel(file2_path, dtype={'借方发生额': 'object', '贷方发生额': 'object'})
            self.update_progress(30)
            self.log(f"已读取原始表2，共 {len(df2)} 行数据")
            # 修改 read_excel 调用，确保金额字段保持高精度

            # 检查必要的列是否存在
            required_columns = ['SAP凭证编号', '单位', '组织机构', '借方发生额', '贷方发生额']
            for col in required_columns:
                if col not in df2.columns:
                    raise ValueError(f"原始表2中缺少必要的列: {col}")

            if 'SAP凭证编号' not in df1.columns:
                raise ValueError("原始表1中缺少必要的列: SAP凭证编号")

            self.update_progress(40)
            self.log("验证列结构完成")

            # 处理步骤1: 筛选表2中SAP凭证编号在表1中存在的数据
            unique_sap_in_df2 = df2['SAP凭证编号'].drop_duplicates()
            sap_in_df1 = set(df1['SAP凭证编号'].unique())

            valid_sap = [sap for sap in unique_sap_in_df2 if sap in sap_in_df1]
            filtered_df2 = df2[df2['SAP凭证编号'].isin(valid_sap)]
            filtered_df2['借方发生额'] = filtered_df2['借方发生额'].apply(lambda x: Decimal(str(x)))
            filtered_df2['贷方发生额'] = filtered_df2['贷方发生额'].apply(lambda x: Decimal(str(x)))
            self.update_progress(55)
            self.log(f"筛选后保留 {len(filtered_df2)} 行数据")

            # 生成带时间戳的文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"加工后的底表_{timestamp}.xlsx"
            output_file = os.path.join(os.path.dirname(file2_path), output_filename)

            # 创建ExcelWriter对象，用于写入多个工作表
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # 写入底表数据
                filtered_df2.to_excel(writer, sheet_name='底表', index=False)
                self.update_progress(65)
                self.log("已生成底表数据")

                # 处理步骤3: 生成输出表 - 按要求生成8列数据
                # 1. 正向汇总：单位作为我方，组织机构作为对方
                # 汇总单位=A且组织机构=a的所有数据
                forward_summary = filtered_df2.groupby(['单位', '组织机构']).agg(
                    借方=('借方发生额', 'sum'),
                    贷方=('贷方发生额', 'sum')
                ).reset_index()
                forward_summary.columns = ['我方', '对方', '借方', '贷方']

                # 2. 反向汇总：组织机构作为我方，单位作为对方
                # 汇总单位=a且组织机构=A的所有数据（实际重新计算）
                reverse_summary = filtered_df2.groupby(['单位', '组织机构']).agg(
                    反向借方=('借方发生额', 'sum'),
                    反向贷方=('贷方发生额', 'sum')
                ).reset_index()
                reverse_summary.columns = ['反向我方', '反向对方', '反向借方', '反向贷方']

                # 3. 合并正向和反向汇总结果
                # 创建一个唯一标识用于合并
                forward_summary['匹配标识'] = forward_summary['我方'] + "_" + forward_summary['对方']
                reverse_summary['匹配标识'] = reverse_summary['反向对方'] + "_" + reverse_summary['反向我方']

                # 合并两个汇总表
                output_table = pd.merge(
                    forward_summary,
                    reverse_summary,
                    on='匹配标识',
                    how='left'
                )

                # 4. 构建8列结构
                output_table = output_table[[
                    '我方', '对方', '借方', '贷方',
                    '反向我方', '反向对方', '反向借方', '反向贷方'
                ]]

                # 5. 重命名列以符合要求
                output_table.columns = [
                    '我方', '对方', '借方', '贷方',
                    '我方', '对方', '借方', '贷方'
                ]

                # 6. 将缺失值填充为0
                output_table = output_table.fillna(0)

                # 写入输出表
                output_table.to_excel(writer, sheet_name='输出表', index=False)

            self.update_progress(90)
            self.log(f"已生成输出表数据，共 {len(output_table)} 行汇总数据")

            self.update_progress(100)
            self.log(f"处理完成！文件已保存至: {output_file}")
            messagebox.showinfo("成功", f"处理完成！\n文件已保存至:\n{output_file}")

        except Exception as e:
            self.log(f"处理出错: {str(e)}")
            messagebox.showerror("错误", f"处理过程中发生错误:\n{str(e)}")
        finally:
            # 恢复处理按钮状态
            self.root.after(0, self.enable_process_button)

    def enable_process_button(self):
        """重新启用处理按钮"""
        for widget in self.root.winfo_children():
            if isinstance(widget, ttk.Button) and widget["text"] == "开始处理":
                widget.config(state=tk.NORMAL)
                break


if __name__ == "__main__":
    root = tk.Tk()
    app = ExcelProcessorApp(root)
    root.mainloop()
