# src/pipeline/parser.py
# -*- coding: utf-8 -*-
# 資料剖析器

import csv
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

def parse_csv_file(file_path, encoding=None, delimiter=","):
    # 剖析CSV檔案，提取表頭和資料列。
    logger.info(f"剖析CSV: {file_path}")
    if not os.path.exists(file_path):
        logger.error(f"CSV檔案不存在: {file_path}")
        return {"header": [], "rows": []}

    resolved_encoding = encoding if encoding else settings.DEFAULT_ENCODING
    header = []
    data_rows = []
    try:
        with open(file_path, "r", encoding=resolved_encoding, newline="") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=delimiter)
            for row in csv_reader: # 尋找表頭
                if any(cell.strip() for cell in row):
                    header = [cell.strip() for cell in row]
                    break
            if not header:
                logger.warning(f"CSV無表頭: {file_path}")
                return {"header": [], "rows": []}
            for row in csv_reader: # 讀取資料
                cleaned_row = [cell.strip() for cell in row]
                if any(cleaned_row):
                    data_rows.append(cleaned_row)
        logger.info(f"CSV剖析完成: {file_path}, 表頭數: {len(header)}, 資料列數: {len(data_rows)}")
        return {"header": header, "rows": data_rows}
    except UnicodeDecodeError as e:
        logger.error(f"CSV編碼錯誤 ({resolved_encoding}) {file_path}: {e}")
        return {"header": [], "rows": []}
    except csv.Error as e:
        logger.error(f"CSV格式錯誤 {file_path}: {e}")
        return {"header": [], "rows": []}
    except Exception as e:
        logger.error(f"剖析CSV時未預期錯誤 {file_path}: {e}", exc_info=True)
        return {"header": [], "rows": []}
