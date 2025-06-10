#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Taifexdtool.py - 期交所資料處理主腳本

此腳本自動化處理資料檔案的下載（模擬）、解析、轉換（預留功能）及資料庫儲存，
最初設計時以處理期交所的 CSV 檔案為目標。
透過 `config.json` 檔案進行設定，並記錄其操作過程。
同時也會產生已處理檔案的摘要報告。
"""
import json
import os
import logging
import csv
import sqlite3
import pytz # 用於時區處理
from datetime import datetime

# 定義台北時區
TAIPEI_TZ = pytz.timezone('Asia/Taipei')

class TaipeiFormatter(logging.Formatter):
    """
    自訂日誌格式化器，使用台北時區並包含毫秒。
    """
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

def load_config(config_path="config.json"):
    """
    從 JSON 檔案載入設定。

    如果指定的設定檔案不存在，則會建立一個包含預設設定的新檔案。
    處理潛在的 JSON 解碼錯誤或檔案找不到的問題，透過印出錯誤訊息並退出程式。

    參數:
        config_path (str, optional): 設定檔案的路徑。
                                     預設為 "config.json"。

    返回:
        dict: 一個包含設定資訊的字典。
              如果載入或建立過程中發生嚴重錯誤，則會退出程式。
    """
    default_config = {
        "database_path": "taifex_data.sqlite",  # SQLite 資料庫的預設路徑
        "log_file": "taifexdtool.log",
        "log_level": "INFO",
        "download_urls": []
    }
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f: # 指定UTF-8讀取
                return json.load(f)
        else:
            with open(config_path, 'w', encoding='utf-8') as f: # 指定UTF-8寫入
                json.dump(default_config, f, indent=2, ensure_ascii=False)
            # 當設定檔是新建立時，印出更詳細的說明訊息
            print(f"新的設定檔 '{config_path}' 已建立。\n"
                  f"您可以在其中設定以下項目：\n"
                  f"  - 'database_path': 資料庫檔案的路徑 (預設: {default_config['database_path']})\n"
                  f"  - 'log_file': 日誌檔案的名稱 (預設: {default_config['log_file']})\n"
                  f"  - 'log_level': 日誌記錄級別 (例如 INFO, DEBUG, WARNING, ERROR, CRITICAL; 預設: {default_config['log_level']})\n"
                  f"  - 'download_urls': 要處理的檔案來源 URL 或本地路徑列表 (預設: 空列表 [])")
            return default_config
    except (json.JSONDecodeError, FileNotFoundError) as e:
        # 日誌系統設定完成後應使用 logging，若發生嚴重問題則暫時使用 print
        print(f"載入或建立設定時發生嚴重錯誤: {e}。程式結束。") # 確保在日誌記錄器未就緒時也能印出
        exit(1) # 設定檔至關重要

def parse_csv_data(csv_content_string, delimiter=','):
    """
    解析 CSV 字串，提取標頭列和資料列。
    此函數會清除儲存格數值的前後多餘空白。

    參數:
        csv_content_string (str): CSV 檔案的字串內容。
        delimiter (str, optional): CSV 中使用的分隔符。預設為 ','。

    返回:
        dict: 一個包含兩個鍵的字典：
              "header" (list): 清理過的標頭列字串列表。
                               若找不到標頭或發生錯誤，則返回空列表。
              "rows" (list of lists): 資料列的列表，其中每個內部列表是
                                      包含清理過的字串數值的資料列。若找不到資料列
                                      或發生錯誤，則返回空列表。
    """
    logger = logging.getLogger(__name__)
    if not csv_content_string or not csv_content_string.strip():
        logger.error("無法解析 CSV：輸入字串為空或僅包含空白。")
        return {"header": [], "rows": []}

    lines = csv_content_string.strip().splitlines()
    if not lines:
        logger.error("無法解析 CSV：去除空白後找不到任何行。")
        return {"header": [], "rows": []}
    
    try:
        # 嘗試尋找第一個非空行作為標頭
        header_index = -1
        for i, line in enumerate(lines):
            if line.strip(): # 檢查該行是否不僅是空白
                header_index = i
                break
        
        if header_index == -1: # 所有行都是空的或空白
            logger.error("無法解析 CSV：所有行都是空的或空白。")
            return {"header": [], "rows": []}

        # 使用 csv.reader 進行穩健的 CSV 解析
        # 標頭行本身
        header_line_content = lines[header_index]
        # csv.reader 預期一個可迭代的行序列，所以將單個標頭行作為列表傳遞
        header_reader = csv.reader([header_line_content], delimiter=delimiter)
        # 清理標頭儲存格
        header = [cell.strip() for cell in next(header_reader)]

        data_rows = []
        # 將後續行作為資料處理
        if len(lines) > header_index + 1:
            data_line_iterable = lines[header_index+1:]
            # 在解析前，從資料列中過濾掉完全空白的行
            non_empty_data_lines = [line for line in data_line_iterable if line.strip()]
            if non_empty_data_lines:
                data_reader = csv.reader(non_empty_data_lines, delimiter=delimiter)
                for row in data_reader:
                    # 確保所有儲存格都被處理，即使它們在解析後是空字串
                    cleaned_row = [cell.strip() if isinstance(cell, str) else cell for cell in row]
                    # 只有當清理後的行並非完全空白時才添加
                    if any(cleaned_row): # 檢查 cleaned_row 中是否有任何非空字串
                         data_rows.append(cleaned_row)
            
        logger.info(f"成功解析 CSV 資料。標頭: {header}。資料列數: {len(data_rows)}")
        return {"header": header, "rows": data_rows}

    except csv.Error as e:
        logger.error(f"解析 CSV 資料時發生錯誤: {e}")
        return {"header": [], "rows": []}
    except Exception as e: # 捕捉任何其他未預期的錯誤
        logger.error(f"CSV 解析過程中發生未預期錯誤: {e}")
        return {"header": [], "rows": []}

def recognize_data_type(file_path, header, first_data_row):
    """
    預留函數，用於識別 CSV 檔案的資料類型。

    此函數旨在擴展邏輯，以便根據檔案路徑、
    標頭內容和第一行資料來區分不同的 CSV 結構
    （例如，特定的期交所報告格式）。

    參數:
        file_path (str): 來源檔案的路徑或識別碼。
        header (list): 代表標頭列的字串列表。
        first_data_row (list): 代表第一行資料的字串列表。

    返回:
        str: 一個表示已識別資料類型的字串（例如 "generic_csv"）。
             目前返回預留值。
    """
    logger = logging.getLogger(__name__)
    logger.info(f"嘗試識別檔案 '{file_path}' 的資料類型。")
    logger.debug(f"用於類型識別的標頭: {header}")
    logger.debug(f"用於類型識別的第一行資料: {first_data_row}")
    
    # 預留邏輯：未來將檢查標頭和資料列內容
    # 以確定特定的期交所 CSV 結構。
    data_type = "generic_csv" 
    logger.info(f"檔案 '{file_path}' 的資料類型被識別為: '{data_type}'")
    return data_type

def transform_data(data_type, parsed_data):
    """
    預留函數，用於根據已識別的類型轉換已解析的資料。

    此函數稍後將更新，以根據 `data_type` 應用特定的資料清理、
    類型轉換或結構轉換。

    參數:
        data_type (str): 由 `recognize_data_type` 決定的資料類型。
        parsed_data (dict): 包含 `parse_csv_data` 結果中 "header" 和 "rows" 的字典。

    返回:
        dict: （可能）轉換後的資料，結構與 `parsed_data` 相同。
              目前按原樣返回資料（傳遞）。
    """
    logger = logging.getLogger(__name__)
    logger.info(f"嘗試轉換類型為 '{data_type}' 的資料。")
    logger.debug(f"用於轉換的資料 (前兩列): 標頭: {parsed_data.get('header')}, 資料列: {parsed_data.get('rows', [])[:2]}")

    # 預留邏輯：未來的實作將根據 data_type 修改 parsed_data。
    # 目前按原樣返回資料。
    transformed_data = parsed_data 
    logger.info(f"類型 '{data_type}' 的資料轉換完成（目前為直接傳遞）。")
    return transformed_data

def generate_summary_report(processed_files_summary):
    """
    記錄腳本執行期間處理的所有檔案的摘要報告。

    報告包括每個檔案的狀態（成功/失敗）、已解析和已插入的列數，
    以及失敗檔案的任何錯誤訊息。同時也提供總體摘要計數。

    參數:
        processed_files_summary (list): 字典列表，其中每個字典
                                        包含單個檔案的處理摘要。
                                        預期鍵包括 'source', 'status',
                                        'rows_parsed', 'rows_inserted', 'error_message'。
    """
    logger = logging.getLogger(__name__)
    logger.info("--- 處理摘要報告 ---")
    
    total_files = len(processed_files_summary)
    successful_files = 0
    failed_files = 0
    
    for summary in processed_files_summary:
        logger.info(f"來源: {summary['source']}")
        logger.info(f"  狀態: {summary['status']}")
        if summary['status'] == "成功": # Changed "Success" to "成功"
            successful_files += 1
            logger.info(f"  已解析列數: {summary.get('rows_parsed', 'N/A')}")
            logger.info(f"  已插入列數: {summary.get('rows_inserted', 'N/A')}")
        else:
            failed_files += 1
            logger.error(f"  錯誤: {summary.get('error_message', '無特定錯誤訊息。')}")
        logger.info("-" * 30)
        
    logger.info("--- 整體摘要 ---")
    logger.info(f"總處理檔案數: {total_files}")
    logger.info(f"成功檔案數: {successful_files}")
    logger.info(f"失敗檔案數: {failed_files}")
    logger.info("--- 報告結束 ---")

def init_db(db_path):
    """
    初始化 SQLite 資料庫。

    連接到由 `db_path` 指定的 SQLite 資料庫。如果資料庫不存在，
    將會被建立。然後確保必要的資料表（例如 `generic_data`）
    如果尚不存在，則會被建立。

    參數:
        db_path (str): SQLite 資料庫的檔案路徑。
    """
    logger = logging.getLogger(__name__)
    logger.info(f"正在初始化資料庫於: {db_path}")
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 建立 generic_data 資料表
        # 將每一列儲存為 JSON 物件，以允許不同 CSV 結構的彈性。
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS generic_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_source TEXT NOT NULL,
                row_number INTEGER NOT NULL,
                data_json TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # 注意：增加了一個時間戳欄位，記錄資料插入的時間。
        
        conn.commit()
        logger.info("資料庫初始化成功。資料表 'generic_data' 已就緒。")
    except sqlite3.Error as e:
        logger.error(f"資料庫初始化期間發生 SQLite 錯誤: {e}")
    finally:
        if conn:
            conn.close()

def insert_data(db_path, file_source, transformed_data):
    """
    將轉換後的資料插入 SQLite 資料庫。

    `transformed_data` 中的每一列都會被轉換成 JSON 字串（將標頭對應到該列的值），
    並儲存在 `generic_data` 資料表中。

    參數:
        db_path (str): SQLite 資料庫的檔案路徑。
        file_source (str): 資料來源的識別碼（例如，檔案名稱）。
        transformed_data (dict): 一個包含 "header"（字串列表）和
                                 "rows"（字串列表的列表）的字典。

    返回:
        tuple: 一個元組 `(success_boolean, count_of_rows_inserted)`。
               如果所有列都已處理且未發生 SQLite 錯誤（即使因為輸入有效但為空而插入 0 列），
               則 `success_boolean` 為 True，否則為 False。
               `count_of_rows_inserted` 是實際插入的列數。
    """
    logger = logging.getLogger(__name__)
    # 首先檢查輸入結構是否有效
    if not transformed_data or not isinstance(transformed_data, dict):
        logger.error(f"來源 '{file_source}' 的 'transformed_data' 輸入無效。應為字典。")
        return False, 0 # 輸入結構嚴重錯誤

    header = transformed_data.get("header")
    rows = transformed_data.get("rows")

    # 如果標頭遺失或不是列表，則表示資料結構有問題。
    if not header or not isinstance(header, list):
        logger.warning(f"無法為 '{file_source}' 插入資料：標頭遺失或不是列表。")
        return False, 0 
    
    # 如果 rows 鍵遺失或其值不是列表，則表示有問題。
    if rows is None or not isinstance(rows, list): # 明確檢查 None 或類型錯誤
        logger.warning(f"無法為 '{file_source}' 插入資料：資料列遺失或不是列表。")
        return False, 0 
        
    # 如果標頭存在，且 rows 是空列表，則不是錯誤；插入 0 列。
    if not rows: 
        logger.info(f"來源 '{file_source}' 無資料列可插入（標頭存在，但資料列列表為空）。")
        return True, 0 # 成功「插入」零列。

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        header = transformed_data["header"]
        rows_inserted_count = 0
        
        for idx, row_values in enumerate(transformed_data["rows"]):
            if len(header) != len(row_values):
                logger.warning(f"略過來源 '{file_source}' 的第 {idx} 列：標頭長度 ({len(header)}) 與資料列長度 ({len(row_values)}) 不符。資料列內容: {row_values}")
                continue

            row_dict = dict(zip(header, row_values))
            try:
                data_json_string = json.dumps(row_dict, ensure_ascii=False) # ensure_ascii=False for Chinese chars
            except TypeError as te:
                logger.error(f"無法將來源 '{file_source}' 的第 {idx} 列序列化為 JSON：{te}。資料列內容: {row_dict}")
                continue # 略過此列

            cursor.execute("""
                INSERT INTO generic_data (file_source, row_number, data_json) 
                VALUES (?, ?, ?)
            """, (file_source, idx, data_json_string))
            rows_inserted_count +=1
            
        conn.commit()
        if rows_inserted_count > 0:
            logger.info(f"已成功從 '{file_source}' 向 '{db_path}' 插入 {rows_inserted_count} 列資料。")
        elif not transformed_data["rows"]: # 一開始就沒有資料列
             logger.info(f"transformed_data 中沒有來源 '{file_source}' 的資料列，因此無內容可插入。")
        else: # 有資料列，但全部被略過
            logger.warning(f"未成功為 '{file_source}' 插入任何資料列（所有資料列可能因錯誤而被略過）。")
        
    except sqlite3.Error as e:
        logger.error(f"為 '{file_source}' 插入資料期間發生 SQLite 錯誤: {e}")
        if conn:
            conn.rollback()
        return False, 0
    except Exception as ex: # 捕捉其他潛在錯誤，例如 zip 或迴圈問題
        logger.error(f"為 '{file_source}' 插入資料期間發生未預期錯誤: {ex}")
        if conn:
            conn.rollback()
        return False, 0
    finally:
        if conn:
            conn.close()
            
    if rows_inserted_count > 0:
        return True, rows_inserted_count
    elif not transformed_data["rows"]: # 無資料列可插入
        return True, 0 
    else: # 有資料列，但無一插入（例如，全部被略過）
        return False, 0


def download_data(url_or_path):
    """
    模擬從 URL 下載資料或從本地檔案路徑讀取資料。

    - 如果 `url_or_path` 以 'http://' 或 'https://' 開頭，則記錄
      實際 URL 下載尚未實作，並返回 `None`。
    - 否則，將 `url_or_path` 視為本地檔案路徑。
      - 如果檔案存在，則讀取並返回其文字內容。
      - 如果檔案不存在或發生 IOError，則記錄錯誤並返回 `None`。

    參數:
        url_or_path (str): 要從中獲取資料的 URL 或本地檔案路徑。

    返回:
        str 或 None: 如果成功，則為檔案內容的字串，否則為 None。
    """
    logger = logging.getLogger(__name__) # 獲取日誌記錄器實例

    if url_or_path.startswith('http://') or url_or_path.startswith('https://'):
        # 實際網路下載邏輯的預留位置
        logger.info(f"尚未實作從 URL '{url_or_path}' 的實際下載功能。")
        return None
    else: # 本地檔案路徑
        if os.path.exists(url_or_path):
            try:
                with open(url_or_path, 'r', encoding='utf-8') as f: # 指定UTF-8讀取
                    content = f.read()
                logger.info(f"已成功從本地檔案「下載」（讀取）資料: {url_or_path}")
                return content
            except IOError as e:
                logger.error(f"讀取本地檔案 '{url_or_path}' 時發生 IOError: {e}")
                return None
        else:
            logger.error(f"找不到本地檔案: {url_or_path}")
            return None

def main():
    """
    Taifexdtool 的主要執行函數。

    協調整個流程：
    1. 載入設定。
    2. 設定日誌記錄。
    3. 初始化資料庫。
    4. 處理設定中指定的每個資料來源（或預設測試來源）。
       對於每個來源：
       a. 下載/讀取資料。
       b. 解析 CSV 資料（如果適用）。
       c. 識別資料類型（預留功能）。
       d. 轉換資料（預留功能）。
       e. 將資料插入資料庫。
    5. 產生所有處理活動的摘要報告。
    """
    # 載入應用程式設定
    config = load_config()
    db_path = config.get("database_path", "taifex_data.sqlite") # 從設定獲取 db_path
    processing_summary_list = [] # 初始化列表以儲存每個已處理檔案的摘要

    # --- 設定日誌 ---
    log_level_str = config.get("log_level", "INFO").upper()
    log_file = config.get("log_file", "taifexdtool.log")
    numeric_log_level = getattr(logging, log_level_str, logging.INFO)

    # 建立格式化器
    # log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s" # 舊格式
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S.%f%z" # 日期格式 含時區
    formatter = TaipeiFormatter(fmt=log_format, datefmt=date_format)


    # 清除已有的 handlers，避免在重跑時重複添加
    # 這對於腳本執行可能不是必要的，但在像Jupyter這樣的環境中可能有用
    root_logger = logging.getLogger()
    if root_logger.handlers:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

    # 對於 __main__ logger 也做一樣的處理
    app_logger = logging.getLogger(__name__)
    if app_logger.handlers:
        for handler in app_logger.handlers[:]:
            app_logger.removeHandler(handler)


    # 設定基礎日誌 (basicConfig 會影響 root logger)
    logging.basicConfig(level=numeric_log_level, format=log_format, handlers=[])

    # 獲取根日誌記錄器並為其設定格式化器和處理器
    # 或者，我們可以只設定我們自己的 logger (__name__)
    logger_to_configure = logging.getLogger(__name__) # 或 logging.getLogger() 來設定 root logger
    logger_to_configure.setLevel(numeric_log_level) # 確保 logger 級別已設定

    # 檔案處理器
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger_to_configure.addHandler(file_handler)

    # 控制台處理器
    console_handler = logging.StreamHandler() # 預設為 sys.stderr，可改為 sys.stdout
    console_handler.setFormatter(formatter)
    logger_to_configure.addHandler(console_handler)

    # 防止日誌事件傳播到根日誌記錄器，如果根日誌記錄器有不想要的處理器
    logger_to_configure.propagate = False


    logger = logging.getLogger(__name__) # 為本模組獲取日誌記錄器

    logger.info(f"設定已載入: {config}")

    # --- 初始化資料庫 ---
    init_db(db_path) # 確保在開始處理前資料庫已就緒。

    # --- 決定要處理的來源 ---
    download_sources = config.get("download_urls", [])
    if not download_sources: # 如果設定中為空，則使用預設測試檔案。
        logger.info("'download_urls' 在設定中為空。使用預設測試檔案進行示範。")
        download_sources = ["sample_data.csv", "http://example.com/nonexistent.csv", "non_existent_file.txt", "empty_file.csv"]

    # 建立一個空的 CSV 檔案以測試僅有標頭的 CSV 情況。
    # 注意：這是在此腳本內進行示範/測試用的。
    # 在生產環境中，測試檔案通常會是專用測試套件的一部分。
    empty_csv_test_file = "empty_file.csv"
    if empty_csv_test_file in download_sources: # 僅當它在要處理的列表中時才建立
        try:
            with open(empty_csv_test_file, "w", encoding='utf-8') as f:
                f.write("欄位A,欄位B\n") # 僅有標頭的最小 CSV
            logger.info(f"已建立 '{empty_csv_test_file}' 用於測試僅有標頭的 CSV 處理。")
        except IOError as e:
            logger.error(f"無法建立 '{empty_csv_test_file}' 進行測試: {e}")
            # 腳本將繼續執行；如果建立失敗，download_data 將處理檔案未找到的情況。

    # --- 主要處理迴圈，針對每個來源 ---
    for source in download_sources:
        # 初始化目前來源的摘要。預設為失敗。
        current_file_summary = {
            'source': source,
            'status': '失敗', # Changed from 'Failed'
            'downloaded_content_size': 0,
            'rows_parsed': 0,
            'rows_inserted': 0,
            'error_message': '處理未開始或過早中斷。'
        }
        logger.info(f"--- 開始處理來源: {source} ---")
        
        # 1. 下載資料
        data_content = download_data(source)
        if data_content is None:
            # download_data 會記錄具體原因（檔案未找到、URL 未實作）。
            # 為摘要擷取一般錯誤。
            if source.startswith('http'):
                current_file_summary['error_message'] = f"下載失敗：URL 下載未實作或無法連線。"
            else: # 本地檔案
                current_file_summary['error_message'] = f"下載失敗：找不到檔案 '{source}' 或無法讀取。"
            logger.warning(f"'{source}' 下載失敗。{current_file_summary['error_message']}")
            processing_summary_list.append(current_file_summary)
            logger.info(f"--- 處理來源結束: {source} (下載失敗) ---")
            continue # 移至下一個來源

        current_file_summary['downloaded_content_size'] = len(data_content)
        logger.info(f"已成功下載/讀取 '{source}'。大小: {len(data_content)} 位元組。")

        # 2. 僅在是 CSV 檔案時處理
        if not source.endswith(".csv"):
            current_file_summary['status'] = '已略過' # Changed 'Skipped'
            current_file_summary['error_message'] = "來源不是 CSV 檔案。"
            logger.info(f"略過非 CSV 來源的 CSV 處理: {source}")
            processing_summary_list.append(current_file_summary)
            logger.info(f"--- 處理來源結束: {source} (已略過非CSV檔案) ---")
            continue

        # 3. 解析 CSV 資料
        logger.info(f"嘗試從 '{source}' 解析 CSV 資料...")
        parsed_data = parse_csv_data(data_content)
        if not parsed_data or not parsed_data.get("header"): # parse_csv_data 在錯誤或空檔案時返回 {"header": [], "rows": []}
            current_file_summary['error_message'] = "解析失敗：找不到標頭或 CSV 無效/為空。"
            logger.warning(f"'{source}' 解析失敗。{current_file_summary['error_message']}")
            processing_summary_list.append(current_file_summary)
            logger.info(f"--- 處理來源結束: {source} (解析失敗) ---")
            continue
        
        current_file_summary['rows_parsed'] = len(parsed_data.get("rows", []))
        logger.info(f"已成功從 '{source}' 解析 CSV。標頭: {parsed_data['header']}。資料列數: {current_file_summary['rows_parsed']}。")

        # 如果解析後無資料列（例如，僅有標頭的 CSV），則此階段視為「成功」。
        if not parsed_data.get("rows"):
            current_file_summary['status'] = '成功' # Changed 'Success' - 已處理檔案，無資料可插入。
            current_file_summary['error_message'] = None # 此情況下無錯誤。
            logger.info(f"來自 '{source}' 的 CSV 有標頭但無資料列可進一步處理或插入。")
            processing_summary_list.append(current_file_summary)
            logger.info(f"--- 處理來源結束: {source} (僅有標頭的CSV) ---")
            continue

        # 4. 識別資料類型（使用第一行資料）
        first_data_row = parsed_data["rows"][0]
        data_type = recognize_data_type(source, parsed_data["header"], first_data_row)
        # 這是一個預留功能，因此尚無特定錯誤處理。

        # 5. 轉換資料
        transformed_data = transform_data(data_type, parsed_data)
        if not transformed_data: # 目前的直接傳遞邏輯下，理想情況不應發生
            current_file_summary['error_message'] = "轉換步驟失敗或未返回任何資料。"
            logger.warning(f"'{source}' 轉換失敗。{current_file_summary['error_message']}")
            processing_summary_list.append(current_file_summary)
            logger.info(f"--- 處理來源結束: {source} (轉換失敗) ---")
            continue
        
        # 6. 將資料插入資料庫
        logger.info(f"嘗試將來自 '{source}' 的資料插入資料庫 '{db_path}'...")
        insert_success, num_inserted = insert_data(db_path, source, transformed_data)
        current_file_summary['rows_inserted'] = num_inserted
        if insert_success:
            current_file_summary['status'] = '成功' # Changed 'Success'
            current_file_summary['error_message'] = None
            logger.info(f"'{source}' 的資料庫插入完成。已插入 {num_inserted} 列。")
        else:
            # insert_data 會記錄特定的 SQLite 錯誤。
            current_file_summary['error_message'] = f"資料庫插入失敗。錯誤前已插入 {num_inserted} 列，若全部失敗則為 0。請檢查日誌。"
            logger.error(f"'{source}' 資料庫插入失敗。{current_file_summary['error_message']}")
            # 狀態保持 '失敗'
        
        processing_summary_list.append(current_file_summary)
        logger.info(f"--- 處理來源結束: {source} ---")

    # --- 產生最終摘要報告 ---
    generate_summary_report(processing_summary_list)

    # 清理測試用的 empty_file.csv（如果已建立）
    if empty_csv_test_file in download_sources and os.path.exists(empty_csv_test_file):
        try:
            os.remove(empty_csv_test_file)
            logger.info(f"已清理測試檔案: '{empty_csv_test_file}'。")
        except OSError as e:
            logger.error(f"移除測試檔案 '{empty_csv_test_file}' 時發生錯誤: {e}")


if __name__ == "__main__":
    main()
