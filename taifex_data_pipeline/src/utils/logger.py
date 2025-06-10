# src/utils/logger.py
# -*- coding: utf-8 -*-

# 日誌記錄器工具

import logging
import sys
import pytz
from datetime import datetime
# 調整導入路徑，以使其在 src 目錄下執行時能正確找到 config.settings
try:
    from ..config import settings
except ImportError:
    # 這種情況可能發生在直接執行此檔案進行測試時，需要調整 sys.path
    # 或者是在 Notebook 中，PYTHONPATH 沒有正確設定時
    # print("無法使用相對導入 ..config，嘗試將 src 加入 sys.path")
    import os
    # 假設 logger.py 在 src/utils/ 下，而 settings.py 在 src/config/ 下
    # 我們需要將 src 的父目錄 (即專案根目錄) 添加到 sys.path
    current_script_path = os.path.abspath(__file__)
    # src/utils/logger.py -> src/utils/ -> src/ -> taifex_data_pipeline/
    project_root_for_logger = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
    # print(f"logger.py: 推測的專案根目錄: {project_root_for_logger}")
    # sys.path.insert(0, project_root_for_logger)
    # print(f"logger.py: sys.path aafter insert: {sys.path}")
    # from src.config import settings # 再次嘗試導入
    # Fallback: 如果在Colab環境直接執行.py檔，且settings.py在預期路徑，則可能需要更複雜的處理
    # 為了簡單起見，這裡假設在Colab Notebook中執行時，PYTHONPATH已由啟動器處理好，或者settings會在orchestrator中傳遞
    # 暫時先讓它在無法導入時失敗，以便在 Notebook 中發現路徑問題
    # 在 Notebook 中，通常會將 src 目錄添加到 sys.path 或使用 %env PYTHONPATH
    # For now, we will assume settings will be available when run from notebook
    # If this script is run directly, it might fail here if not structured for it.
    pass # 允許它失敗，以便在 Notebook 中調試路徑

TAIPEI_TZ = pytz.timezone('Asia/Taipei')

class TaipeiFormatter(logging.Formatter):
    '''自訂日誌格式化器，使用台北時區'''
    def converter(self, timestamp):
        dt = datetime.fromtimestamp(timestamp, tz=pytz.utc)
        return dt.astimezone(TAIPEI_TZ)

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec="milliseconds")
            except TypeError:
                s = dt.isoformat()
        return s

_logger = None # 模組級別的日誌記錄器實例

def setup_logger(log_file_path=None, log_level=None): # 允許傳入參數以覆蓋 settings
    '''設定並返回一個日誌記錄器'''
    global _logger
    if _logger is not None:
        # print("Logger already configured") # for debugging
        return _logger

    logger = logging.getLogger("TaifexPipeline") # 建立一個名為 TaifexPipeline 的 logger
    logger.handlers = [] # 清除已有的 handlers，避免重複添加 (尤其在 Colab 重跑 cell 時)

    # 優先使用傳入的參數，否則從 settings 模組讀取
    current_log_level_str = log_level if log_level else settings.LOG_LEVEL.upper()
    current_log_file_path = log_file_path if log_file_path else settings.LOG_FILE_PATH

    numeric_log_level = getattr(logging, current_log_level_str, logging.INFO)
    logger.setLevel(numeric_log_level)

    # 設定日誌格式
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S.%f%z'
    formatter = TaipeiFormatter(fmt=log_format, datefmt=date_format)

    # 設定控制台處理器 (StreamHandler)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 設定檔案處理器 (FileHandler)
    try:
        file_handler = logging.FileHandler(current_log_file_path, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.error(f'無法設定日誌檔案處理器於 {current_log_file_path}: {e}', exc_info=True)

    # logger.info(f"日誌記錄器 '{logger.name}' 設定完成。日誌級別: {current_log_level_str}。日誌檔案路徑: {current_log_file_path}")
    _logger = logger # 將設定好的 logger 賦值給模組級變數
    return _logger

def get_logger():
    '''獲取已設定的日誌記錄器。如果尚未設定，則先進行設定。'''
    if _logger is None:
        # print("Logger not set, calling setup_logger()") # for debugging
        return setup_logger()
    return _logger

# --- 用於測試 logger.py 是否能獨立運行的程式碼 ---
# if __name__ == "__main__":
#     print("Running logger.py directly for testing...")
#     # 為了在直接執行 logger.py 時能夠找到 settings，需要調整 sys.path
#     import os
#     # 假設 logger.py 在 src/utils/ 下，而 settings.py 在 src/config/ 下
#     # 我們需要將 src 的父目錄 (即 taifex_data_pipeline) 添加到 sys.path
#     current_script_path = os.path.abspath(__file__)
#     # src/utils/logger.py -> src/utils/ -> src/ -> taifex_data_pipeline/
#     project_root_for_logger_test = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
#     sys.path.insert(0, project_root_for_logger_test)
#     print(f"Adjusted sys.path for direct logger test: {sys.path}")
#     from src.config import settings as test_settings
#     print(f"Test settings LOG_FILE_PATH: {test_settings.LOG_FILE_PATH}")
#     # 測試時可以指定一個不同的日誌檔案，以避免與主應用程式的日誌混淆
#     test_log_file = os.path.join(test_settings.PROJECT_ROOT, "test_logger.log")
#     print(f"Test log file will be: {test_log_file}")
#     # 清理舊的測試日誌檔案
#     if os.path.exists(test_log_file): os.remove(test_log_file)
#     logger = setup_logger(log_file_path=test_log_file, log_level="DEBUG")
#     logger.debug("這是一條 DEBUG 日誌訊息 (如果級別夠低才會顯示)")
#     logger.info("這是一條 INFO 日誌訊息")
#     logger.warning("這是一條 WARNING 日誌訊息")
#     logger.error("這是一條 ERROR 日誌訊息")
#     logger.critical("這是一條 CRITICAL 日誌訊息")
#     try:
#         1/0
#     except ZeroDivisionError:
#         logger.error('捕捉到一個錯誤', exc_info=True)
#     print(f"Test log content should be in: {test_log_file}")
