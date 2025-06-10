# src/orchestrator.py
# -*- coding: utf-8 -*-
# 總指揮：協調整個資料處理流程

import os
import shutil # 用於清理臨時目錄

try:
    from .utils.logger import get_logger
    from .utils import reporter
    from .pipeline import file_handler
    from .pipeline import parser
    from .pipeline import database
    from .config import settings
except ImportError:
    # Fallback for direct execution or testing
    import sys
    current_script_path = os.path.abspath(__file__)
    project_root_for_direct_run = os.path.dirname(os.path.dirname(current_script_path)) # Moves up to taifex_data_pipeline
    if project_root_for_direct_run not in sys.path:
        sys.path.insert(0, project_root_for_direct_run)
    from src.utils.logger import get_logger # type: ignore
    from src.utils import reporter # type: ignore
    from src.pipeline import file_handler # type: ignore
    from src.pipeline import parser # type: ignore
    from src.pipeline import database # type: ignore
    from src.config import settings # type: ignore

logger = get_logger()

def process_single_file(uploaded_file_path, db_path, temp_base_dir):
    # 處理單一上傳檔案的完整流程
    # Args:
    #     uploaded_file_path (str): 已上傳到伺服器臨時位置的檔案路徑
    #     db_path (str): 資料庫檔案路徑
    #     temp_base_dir (str): 用於存放解壓縮和處理中檔案的基礎臨時目錄
    # Returns:
    #     bool: True 表示整體處理成功，False 表示處理過程中出現任何關鍵錯誤

    original_filename = os.path.basename(uploaded_file_path)
    file_size = None
    if os.path.exists(uploaded_file_path):
        file_size = os.path.getsize(uploaded_file_path)

    # 步驟0: 初始化資料庫 (如果尚未進行)
    # 在實際應用中，init_db 可能在應用啟動時執行一次即可
    # 此處為確保每次處理檔案時資料庫都可用
    if not database.init_db(db_path):
        logger.error(f"總指揮: 資料庫初始化失敗於 {db_path}，無法繼續處理檔案 {original_filename}")
        # 可以在此處使用 reporter 產生一個更面向使用者的錯誤訊息，但目前主要依賴日誌
        return False # 指示處理失敗

    reporter.generate_initial_report_banner(original_filename, file_size)
    overall_success = True # 追蹤整體處理狀態
    processed_data_summary = [] # 用於最終報告的摘要資訊

    # 清理/建立本次檔案處理專用的臨時目錄
    # 使用原始檔名（不含副檔名）作為子目錄，避免衝突
    current_file_temp_processing_dir = os.path.join(temp_base_dir, os.path.splitext(original_filename)[0] + "_proc")
    if os.path.exists(current_file_temp_processing_dir):
        shutil.rmtree(current_file_temp_processing_dir)
    os.makedirs(current_file_temp_processing_dir, exist_ok=True)
    logger.info(f"為檔案 [{original_filename}] 建立的處理中臨時目錄: [{current_file_temp_processing_dir}]")

    # 步驟 1: 智慧型檔案識別與提取
    step_num = 1; total_steps = 4 # 假設總共4個主要步驟
    step_name_1 = "檔案內容智慧型識別與提取"
    detected_mime, extracted_files = file_handler.handle_uploaded_file(uploaded_file_path, current_file_temp_processing_dir)

    step_1_details = {}
    if file_size is not None: step_1_details["原始檔案大小"] = f"{file_size / (1024*1024):.2f} MB" if file_size > 0 else f"{file_size} Bytes"
    step_1_details["偵測到的檔案類型 (MIME)"] = detected_mime

    if detected_mime and not detected_mime.startswith("error/"): # 表示MIME偵測本身是成功的
        if extracted_files:
            file_category = settings.SUPPORTED_MIME_TYPES.get(detected_mime, "未知壓縮類型")
            if file_category in ["zip", "rar", "7z", "gz", "tar", "bz2"]:
                msg = f"成功: 已將檔案識別為 {detected_mime} 並解壓縮. 發現 {len(extracted_files)} 個檔案."
                step_1_details["解壓縮後檔案"] = [os.path.basename(f) for f in extracted_files]
            else: # text, csv
                msg = f"成功: 已將檔案識別為 {detected_mime}. 檔案已準備就緒."
            reporter.generate_step_report(step_num, total_steps, step_name_1, True, msg, step_1_details)
        elif detected_mime in settings.SUPPORTED_MIME_TYPES and settings.SUPPORTED_MIME_TYPES[detected_mime] not in ["text", "csv"]:
             # 是支援的壓縮檔 但解壓後為空
             msg = f"警告: 檔案識別為 {detected_mime} 但解壓縮後未發現任何檔案."
             reporter.generate_step_report(step_num, total_steps, step_name_1, False, msg, step_1_details)
             overall_success = False # 標記為部分失敗
        elif detected_mime in settings.SUPPORTED_MIME_TYPES and settings.SUPPORTED_MIME_TYPES[detected_mime] in ["text", "csv"] and not extracted_files:
             # 是文本CSV 但複製失敗 (理論上 file_handler 內部會處理並返回空列表)
             msg = f"錯誤: 檔案識別為 {detected_mime} 但處理 (複製) 失敗."
             reporter.generate_step_report(step_num, total_steps, step_name_1, False, msg, step_1_details)
             overall_success = False
        else: # 不支援的MIME類型
            msg = f"資訊: 檔案類型 {detected_mime} 非直接支援的壓縮或文本格式 無法自動解析."
            reporter.generate_step_report(step_num, total_steps, step_name_1, True, msg, step_1_details)
            # overall_success 保持不變 因為只是資訊提示 非錯誤
    else: # MIME 偵測失敗或檔案不可讀
        msg = f"失敗: {detected_mime}" # detected_mime 此時會是 "error/..."
        reporter.generate_step_report(step_num, total_steps, step_name_1, False, msg, step_1_details)
        overall_success = False

    # 步驟 2, 3, 4: 解析與儲存 (僅當上一步驟成功且有檔案時)
    if overall_success and extracted_files:
        for idx, file_to_parse_path in enumerate(extracted_files):
            file_to_parse_name = os.path.basename(file_to_parse_path)
            logger.info(f"準備處理由 [{original_filename}] 產生的檔案: [{file_to_parse_name}]")

            # 假設所有解壓出來的或直接提供的檔案 若要解析 都是CSV
            # 未來可以加入更複雜的判斷 或根據副檔名篩選
            # 此處簡化為：如果原始檔案是壓縮檔 解壓出的檔案都嘗試解析
            # 如果原始檔案是CSV/TXT 則 extracted_files 只有它自己
            # Corrected line: used file_handler.get_file_mime_type
            is_likely_csv = file_to_parse_name.lower().endswith(".csv") or "text" in file_handler.get_file_mime_type(file_to_parse_path)

            if not is_likely_csv:
                logger.info(f"檔案 [{file_to_parse_name}] 非CSV格式 跳過解析與儲存步驟")
                processed_data_summary.append({"source_filename": file_to_parse_name, "status": "跳過", "message": "非CSV格式或無法解析的文本檔案"})
                continue

            # 步驟 2: 格式分析與範本匹配 (目前簡化)
            step_num_parse = 2 + idx * 2 # 每個檔案有解析和儲存兩個子步驟
            step_name_2 = f"格式分析與範本匹配 ({file_to_parse_name})"
            parsed_data = parser.parse_csv_file(file_to_parse_path) # 使用預設編碼和分隔符
            data_type_recognized = "generic_csv" # 初期固定範本

            step_2_details = {"檔案名稱": file_to_parse_name}
            if parsed_data["header"]:
                # Corrected f-string:
                msg_parse = f"成功: 已成功識別資料格式為 {data_type_recognized} 解析到 {len(parsed_data['rows'])} 行資料"
                step_2_details["匹配範本"] = data_type_recognized
                step_2_details["解析行數"] = len(parsed_data["rows"])
                reporter.generate_step_report(step_num_parse, total_steps, step_name_2, True, msg_parse, step_2_details)
            else:
                msg_parse = "失敗: 無法解析CSV內容或未找到表頭"
                reporter.generate_step_report(step_num_parse, total_steps, step_name_2, False, msg_parse, step_2_details)
                overall_success = False
                processed_data_summary.append({"source_filename": file_to_parse_name, "status": "失敗", "message": msg_parse, "details": step_2_details})
                continue # 此檔案解析失敗 繼續處理下一個解壓出的檔案

            # 步驟 3: 資料清洗與儲存 (目前簡化 無清洗步驟)
            step_num_store = step_num_parse + 1
            step_name_3 = f"資料儲存 ({file_to_parse_name})"
            # data_type_recognized 來自上一步
            db_success, rows_inserted = database.insert_structured_data(db_path, file_to_parse_name, data_type_recognized, parsed_data["header"], parsed_data["rows"])

            step_3_details = {"目標資料庫表": "generic_data"}
            if db_success:
                msg_store = f"成功: {rows_inserted} 行資料已儲存到資料庫"
                step_3_details["儲存行數"] = rows_inserted
                reporter.generate_step_report(step_num_store, total_steps, step_name_3, True, msg_store, step_3_details)
                processed_data_summary.append({"source_filename": file_to_parse_name, "status": "成功", "message": msg_store, "details": step_3_details})
            else:
                msg_store = "失敗: 資料儲存至資料庫時發生錯誤"
                step_3_details["儲存行數"] = rows_inserted # 可能部分插入後失敗
                reporter.generate_step_report(step_num_store, total_steps, step_name_3, False, msg_store, step_3_details)
                overall_success = False # 標記整體失敗
                processed_data_summary.append({"source_filename": file_to_parse_name, "status": "失敗", "message": msg_store, "details": step_3_details})
    elif not overall_success:
        logger.warning(f"由於檔案識別/提取失敗 檔案 [{original_filename}] 的後續解析和儲存步驟已跳過")
        processed_data_summary.append({"source_filename": original_filename, "status": "失敗", "message": "檔案識別或提取失敗 導致無法處理"})
    else: # overall_success is True but no extracted_files (e.g. unsupported MIME type was handled gracefully)
        logger.info(f"檔案 [{original_filename}] (MIME: {detected_mime}) 未產生需解析的檔案 無後續步驟")
        if not detected_mime.startswith("error/"): # 如果不是之前的錯誤 而是不支援類型
             processed_data_summary.append({"source_filename": original_filename, "status": "跳過", "message": f"檔案類型 {detected_mime} 無需解析或儲存"})

    # 步驟 4: 最終報告 (橫幅)
    reporter.generate_final_report_banner(original_filename, overall_success)

    # 清理為此特定檔案建立的臨時處理目錄 (current_file_temp_processing_dir)
    # 注意: temp_base_dir (例如 temp_uploads) 通常在 Colab Notebook 的更高層級進行管理和清理
    try:
        if os.path.exists(current_file_temp_processing_dir):
            shutil.rmtree(current_file_temp_processing_dir)
            logger.info(f"已清理臨時處理目錄: [{current_file_temp_processing_dir}]")
    except Exception as e_cleanup:
        logger.error(f"清理臨時目錄 [{current_file_temp_processing_dir}] 時發生錯誤: {e_cleanup}", exc_info=True)

    return overall_success, processed_data_summary # 返回整體成功狀態和摘要
