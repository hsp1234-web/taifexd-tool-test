# src/pipeline/database.py
# -*- coding: utf-8 -*-
# 資料庫管理器 負責資料庫初始化與資料儲存

import sqlite3
import json
import os

try:
    from ..utils.logger import get_logger
    from ..config import settings
except ImportError:
    import sys
    current_script_path = os.path.abspath(__file__)
    project_root_for_direct_run = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
    if project_root_for_direct_run not in sys.path:
        sys.path.insert(0, project_root_for_direct_run)
    from src.utils.logger import get_logger # type: ignore
    from src.config import settings # type: ignore

logger = get_logger()

def init_db(db_path):
    # 初始化資料庫 若不存在則建立相關表格
    logger.info(f"開始初始化資料庫於路徑 {db_path}")
    conn = None
    try:
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"已建立資料庫目錄 {db_dir}")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # 建立一個通用的資料表 generic_data
        # file_source 原始檔案名或標識符
        # data_type 資料類型 例如 futures_daily options_daily
        # row_content_json 將每一列的數據轉為 JSON 字串儲存
        # imported_at 資料導入時間戳
        create_table_sql = ''''
            CREATE TABLE IF NOT EXISTS generic_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_source TEXT NOT NULL,
                data_type TEXT,
                row_number INTEGER NOT NULL,
                row_content_json TEXT NOT NULL,
                imported_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        '''
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info(f"資料庫 [{db_path}] 初始化成功 表格 generic_data 已準備就緒")
        return True
    except sqlite3.Error as e:
        logger.error(f"初始化資料庫 [{db_path}] 時發生 SQLite 錯誤 {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"初始化資料庫 [{db_path}] 時發生未預期錯誤 {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()

def insert_structured_data(db_path, file_source, data_type, header, rows):
    # 將結構化資料插入資料庫
    logger.info(f"準備將來源 [{file_source}] 型態 [{data_type}] 資料插入資料庫 [{db_path}]")
    if not header:
        logger.warning(f"來源 [{file_source}] 表頭為空 無法插入")
        return False, 0
    if not rows:
        logger.info(f"來源 [{file_source}] 無資料行可插入")
        return True, 0

    conn = None
    inserted_count = 0
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for i, row_values in enumerate(rows):
            if len(header) != len(row_values):
                logger.warning(f"來源 [{file_source}] 第 {i+1} 行欄位數 ({len(row_values)}) 與表頭 ({len(header)}) 不符 跳過此行")
                continue

            row_dict = dict(zip(header, row_values))
            try:
                row_json = json.dumps(row_dict, ensure_ascii=False)
            except TypeError as te:
                logger.error(f"來源 [{file_source}] 第 {i+1} 行序列化JSON失敗 {te} 資料 {row_dict}", exc_info=True)
                continue

            try:
                insert_sql = ''''
                    INSERT INTO generic_data (file_source, data_type, row_number, row_content_json)
                    VALUES (?, ?, ?, ?)
                '''
                cursor.execute(insert_sql, (file_source, data_type, i + 1, row_json))
                inserted_count += 1
            except sqlite3.Error as db_err:
                logger.error(f"插入來源 [{file_source}] 第 {i+1} 行至資料庫SQLite錯誤 {db_err} 資料 {row_json}", exc_info=True)

        conn.commit()
        if inserted_count > 0 :
            logger.info(f"成功從來源 [{file_source}] ({data_type}) 插入 {inserted_count} 行資料到 [{db_path}]")
        elif not rows:
            logger.info(f"來源 [{file_source}] ({data_type}) 中沒有資料行可插入")
        else:
            logger.warning(f"來源 [{file_source}] ({data_type}) 未成功插入任何資料列")

        return True, inserted_count
    except sqlite3.Error as e:
        logger.error(f"資料庫操作失敗 ({db_path}) 來源 [{file_source}] {e}", exc_info=True)
        if conn: conn.rollback()
        return False, inserted_count
    except Exception as e:
        logger.error(f"插入資料到資料庫 [{db_path}] 時發生未預期錯誤 ({file_source}) {e}", exc_info=True)
        if conn: conn.rollback()
        return False, inserted_count
    finally:
        if conn:
            conn.close()
