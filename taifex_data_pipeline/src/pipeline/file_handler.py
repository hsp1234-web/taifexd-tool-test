# src/pipeline/file_handler.py
# -*- coding: utf-8 -*-
# 智慧型檔案處理器：負責識別檔案類型並處理壓縮檔

import os
import shutil
import magic # python-magic 函式庫
import patoolib # patool 函式庫 用於解壓縮多種格式

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

def get_file_mime_type(file_path):
    # 使用 python-magic 函式庫來獲取檔案的 MIME 類型
    # Args:
    #     file_path (str): 要檢查的檔案路徑
    # Returns:
    #     str or None: 檔案的 MIME 類型字串
    #                  如果檔案不存在 不可讀或發生偵測錯誤則返回 None
    if not os.path.exists(file_path):
        logger.error(f"檔案 [{file_path}] 不存在 無法偵測 MIME 類型")
        return None
    if not os.access(file_path, os.R_OK):
        logger.error(f"檔案 [{file_path}] 無法讀取 (權限不足) 無法偵測 MIME 類型")
        return None
    try:
        mime_type = magic.from_file(file_path, mime=True)
        logger.info(f"檔案 [{os.path.basename(file_path)}] 的 MIME 類型偵測為: {mime_type}")
        return mime_type
    except magic.MagicException as e:
        logger.error(f"使用 python-magic 偵測檔案 [{os.path.basename(file_path)}] MIME 類型時發生 MagicException: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"偵測檔案 [{os.path.basename(file_path)}] MIME 類型時發生未預期錯誤: {e}", exc_info=True)
        return None

def extract_archive(archive_path, output_directory):
    # 使用 patoolib 解壓縮指定的壓縮檔案到指定的目錄
    # Args:
    #     archive_path (str): 壓縮檔案的路徑
    #     output_directory (str): 解壓縮檔案存放的目標目錄 此目錄如果不存在會被創建
    # Returns:
    #     list[str]: 解壓縮後所有檔案的絕對路徑列表 (遞歸獲取) 如果解壓縮失敗則返回空列表
    extracted_files_list = []
    try:
        base_archive_name = os.path.basename(archive_path)
        logger.info(f"開始解壓縮檔案 [{base_archive_name}] 到目錄 [{output_directory}]")
        if os.path.exists(output_directory):
            shutil.rmtree(output_directory)
        os.makedirs(output_directory, exist_ok=True)
        patoolib.extract_archive(archive_path, outdir=output_directory, verbosity=-1)
        logger.info(f"檔案 [{base_archive_name}] 解壓縮命令執行完畢")
        for root, _, files in os.walk(output_directory):
            for file_name in files:
                if not file_name.startswith("."): # 忽略隱藏檔案
                    full_path = os.path.join(root, file_name)
                    extracted_files_list.append(os.path.abspath(full_path))
        if not extracted_files_list:
            logger.warning(f"解壓縮檔案 [{base_archive_name}] 後 在目錄 [{output_directory}] 未找到任何檔案")
        else:
            logger.debug(f"解壓縮後在 [{output_directory}] 找到 {len(extracted_files_list)} 個檔案")
        return extracted_files_list
    except patoolib.util.PatoolError as e:
        logger.error(f"patoolib 解壓縮檔案 [{os.path.basename(archive_path)}] 失敗: {e}", exc_info=True)
        if os.path.exists(output_directory): shutil.rmtree(output_directory, ignore_errors=True)
        return []
    except Exception as e:
        logger.error(f"解壓縮檔案 [{os.path.basename(archive_path)}] 時發生未預期錯誤: {e}", exc_info=True)
        if os.path.exists(output_directory): shutil.rmtree(output_directory, ignore_errors=True)
        return []

def handle_uploaded_file(source_file_path, temp_processing_dir_base):
    # 處理上傳的檔案 識別其MIME類型 並根據類型處理
    # Args:
    #     source_file_path (str): 上傳的原始檔案路徑
    #     temp_processing_dir_base (str): 存放處理後檔案的基礎臨時目錄
    # Returns:
    #     tuple[str, list[str]]: (偵測到的MIME類型字串 處理後的檔案絕對路徑列表)
    #                            MIME類型: "error/file-not-accessible" "error/mime-detection-failed" 或實際MIME type
    base_filename = os.path.basename(source_file_path)
    filename_no_ext, _ = os.path.splitext(base_filename)
    logger.info(f"處理檔案: [{base_filename}], 基礎臨時目錄: [{temp_processing_dir_base}]")
    if not os.path.exists(source_file_path) or not os.access(source_file_path, os.R_OK):
        logger.error(f"來源檔案 [{source_file_path}] 不存在或不可讀")
        return "error/file-not-accessible", []
    os.makedirs(temp_processing_dir_base, exist_ok=True)
    mime_type = get_file_mime_type(source_file_path)
    if mime_type is None:
        logger.warning(f"檔案 [{base_filename}] MIME 類型偵測失敗")
        return "error/mime-detection-failed", []
    processed_file_paths = []
    if mime_type in settings.SUPPORTED_MIME_TYPES:
        file_category = settings.SUPPORTED_MIME_TYPES[mime_type]
        logger.info(f"檔案 [{base_filename}] 分類為: {file_category} (MIME: {mime_type})")
        if file_category in ["zip", "rar", "7z", "gz", "tar", "bz2"]:
            specific_extraction_dir = os.path.join(temp_processing_dir_base, filename_no_ext)
            logger.info(f"壓縮檔案 [{base_filename}] 將解壓到: [{specific_extraction_dir}]")
            processed_file_paths = extract_archive(source_file_path, specific_extraction_dir)
            if not processed_file_paths:
                logger.warning(f"檔案 [{base_filename}] (MIME: {mime_type}) 解壓後無檔案")
                return mime_type, []
        elif file_category in ["text", "csv"]:
            try:
                destination_path = os.path.join(temp_processing_dir_base, base_filename)
                if os.path.abspath(source_file_path) == os.path.abspath(destination_path):
                    logger.info(f"來源檔案 [{source_file_path}] 與目標路徑 [{destination_path}] 相同 無需複製")
                else:
                    shutil.copy2(source_file_path, destination_path)
                    logger.info(f"檔案 [{base_filename}] (MIME: {mime_type}) 已複製到 [{destination_path}]")
                processed_file_paths.append(os.path.abspath(destination_path))
            except Exception as e:
                logger.error(f"複製檔案 [{base_filename}] 到 [{temp_processing_dir_base}] 失敗: {e}", exc_info=True)
                return mime_type, []
        else:
            logger.error(f"MIME類型 {mime_type} ({base_filename}) 在支援列表但無處理邏輯")
            return mime_type, []
    else:
        logger.warning(f"檔案 [{base_filename}] MIME類型 ({mime_type}) 不支援處理")
        return mime_type, []
    if processed_file_paths:
        logger.info(f"檔案 [{base_filename}] 處理完成 產生 {len(processed_file_paths)} 個檔案")
    else:
        logger.info(f"檔案 [{base_filename}] (MIME: {mime_type}) 未產生輸出檔案")
    return mime_type, processed_file_paths
