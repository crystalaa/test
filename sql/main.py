# main.py
import sys
import traceback
import logging
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication
from ui_components import ExcelComparer, exception_hook
from utils import resource_path

# 配置日志记录器
logging.basicConfig(
    filename="./error_log.txt",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    sys.excepthook = exception_hook
    app = QApplication(sys.argv)
    icon_path = resource_path('icon.ico')
    app.setWindowIcon(QIcon(icon_path))
    ex = ExcelComparer()
    ex.show()
    exit_code = app.exec_()
    sys.exit(exit_code)
