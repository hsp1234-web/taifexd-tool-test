# src/config/settings.py
# -*- coding: utf-8 -*-
# 主要設定檔案

import os

# 專案根目錄 (自動偵測)
# 假設 settings.py 位於 taifex_data_pipeline/src/config/
# 因此 專案根目錄是 config/ 的上兩層目錄
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 資料庫相關設定
# 資料庫檔案名稱
DATABASE_NAME = "taifex_data.sqlite"
# 資料庫完整路徑
DATABASE_PATH = os.path.join(PROJECT_ROOT, DATABASE_NAME)

# 日誌相關設定
# 日誌檔案名稱
LOG_FILE_NAME = "taifex_pipeline.log"
# 日誌檔案完整路徑
LOG_FILE_PATH = os.path.join(PROJECT_ROOT, LOG_FILE_NAME)
# 日誌級別 (DEBUG INFO WARNING ERROR CRITICAL)
LOG_LEVEL = "INFO"

# 臨時檔案目錄設定
# 上傳檔案存放的臨時目錄名稱
TEMP_UPLOAD_DIR_NAME = "temp_uploads"
# 上傳檔案存放的臨時目錄完整路徑
TEMP_UPLOAD_DIR = os.path.join(PROJECT_ROOT, TEMP_UPLOAD_DIR_NAME)
# 解壓縮檔案存放的臨時目錄名稱
TEMP_EXTRACTION_DIR_NAME = "temp_extracted"
# 解壓縮檔案存放的臨時目錄完整路徑
TEMP_EXTRACTION_DIR = os.path.join(PROJECT_ROOT, TEMP_EXTRACTION_DIR_NAME)

# 支援的MIME類型 (用於 file_handler.py)
SUPPORTED_MIME_TYPES = {
    # 壓縮檔案
    "application/zip": "zip",
    "application/x-zip-compressed": "zip",
    "application/x-rar-compressed": "rar",
    "application/x-rar": "rar",
    "application/x-7z-compressed": "7z",
    "application/gzip": "gz",
    "application/x-gzip": "gz",
    "application/x-tar": "tar",
    "application/x-bzip2": "bz2",
    # 文字/CSV 檔案
    "text/plain": "text",
    "text/csv": "csv",
    "application/octet-stream": "binary", # 可選的 通用二進制
    "application/vnd.ms-excel": "csv", # 有時CSV會被識別为此MIME
    "application/x-empty": "text", # 空檔案的可能MIME類型
    "inode/x-empty": "text" # 空檔案的可能MIME類型
}

# 檔案處理相關設定
# 預設檔案編碼
DEFAULT_ENCODING = "utf-8"
