# src/utils/logger.py
# -*- coding: utf-8 -*-
# 日誌記錄器工具

import logging
import sys
import pytz # 用於時區處理
from datetime import datetime

try:
    from ..config import settings # 從上一層的config模組導入settings
except ImportError:
    # Fallback for direct execution or testing if PYTHONPATH is not set
    import os
    current_script_path = os.path.abspath(__file__)
    project_root_for_direct_run = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
    if project_root_for_direct_run not in sys.path:
        sys.path.insert(0, project_root_for_direct_run)
    from src.config import settings # type: ignore

TAIPEI_TZ = pytz.timezone('Asia/Taipei') # 定義台北時區

class TaipeiFormatter(logging.Formatter):
    # 自訂日誌格式化器 使用台北時區
    def converter(self, timestamp):
        dt = datetime.fromtimestamp(timestamp, tz=pytz.utc) # 從時間戳轉換並設為UTC
        return dt.astimezone(TAIPEI_TZ) # 轉換為台北時區

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created) # 使用轉換後的台北時間
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec="milliseconds") # ISO格式 含毫秒
            except TypeError: # 老版本Python可能不支持timespec
                s = dt.isoformat()
        return s

_logger = None # 模組級別的日誌記錄器實例 避免重複設定

def setup_logger(log_file_path=None, log_level=None):
    # 設定並返回一個日誌記錄器
    global _logger
    if _logger is not None:
        return _logger # 如果已設定 直接返回

    logger_instance = logging.getLogger("TaifexPipeline") # 獲取名為TaifexPipeline的logger
    logger_instance.handlers = [] # 清除已有的handlers 避免在Colab重跑時重複添加

    # 優先使用傳入參數 若無則從settings讀取
    current_log_level_str = log_level if log_level else settings.LOG_LEVEL.upper()
    current_log_file_path = log_file_path if log_file_path else settings.LOG_FILE_PATH

    numeric_log_level = getattr(logging, current_log_level_str, logging.INFO)
    logger_instance.setLevel(numeric_log_level)

    # 設定日誌格式
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S.%f%z" # 日期格式 含時區
    formatter = TaipeiFormatter(fmt=log_format, datefmt=date_format)

    # 設定控制台處理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger_instance.addHandler(console_handler)

    # 設定檔案處理器
    try:
        file_handler = logging.FileHandler(current_log_file_path, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger_instance.addHandler(file_handler)
    except Exception as e:
        logger_instance.error(f"無法設定日誌檔案處理器於 {current_log_file_path}: {e}", exc_info=True)

    logger_instance.info(f"日誌記錄器 [{logger_instance.name}] 設定完成. 日誌級別: {current_log_level_str}. 日誌檔案: {current_log_file_path}")
    _logger = logger_instance # 賦值給模組級變數
    return _logger

def get_logger():
    # 獲取已設定的日誌記錄器 如果尚未設定則先進行設定
    if _logger is None:
        return setup_logger()
    return _logger
