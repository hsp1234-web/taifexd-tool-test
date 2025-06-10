# src/utils/reporter.py
# -*- coding: utf-8 -*-
# 報告工具：負責產生格式化的中文處理報告

import os
from datetime import datetime # 用於獲取當前時間，配合logger的時區

try:
    from .logger import get_logger, TAIPEI_TZ # 從同層級的logger模組導入
except ImportError:
    # Fallback for direct execution or testing
    import sys
    current_script_path = os.path.abspath(__file__)
    # Assuming this script is in src/utils/reporter.py, to get to project root:
    # src/utils/reporter.py -> src/utils/ -> src/ -> project_root
    project_root_for_direct_run = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
    if project_root_for_direct_run not in sys.path:
        sys.path.insert(0, project_root_for_direct_run)
    from src.utils.logger import get_logger, TAIPEI_TZ # type: ignore

logger = get_logger()

def get_current_taipei_time_str():
    # 獲取當前台北時間的格式化字串
    return datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

def generate_initial_report_banner(original_filename, file_size_bytes=None):
    # 產生處理開始的橫幅報告
    timestamp = get_current_taipei_time_str()
    size_str = f"{file_size_bytes / (1024*1024):.2f} MB" if isinstance(file_size_bytes, (int, float)) else "未知大小"
    report_lines = []
    report_lines.append("="*60)
    report_lines.append(f"[{timestamp}] \U0001F4C4 開始處理上傳項目: {original_filename}")
    report_lines.append(f"      - 原始檔案大小: {size_str}")
    report_lines.append("-"*60)
    # Log and print
    for line in report_lines: logger.info(line)
    return "\n".join(report_lines)

def generate_step_report(step_number, total_steps, step_name, success, message, details=None):
    # 為單一步驟產生報告條目
    # Args:
    # step_number (int): 當前步驟編號
    # total_steps (int): 總步驟數
    # step_name (str): 步驟名稱 (例如 "檔案內容智慧型識別")
    # success (bool): 此步驟是否成功
    # message (str): 關於此步驟結果的簡短訊息
    # details (dict or list, optional): 額外的詳細資訊 例如 {"檔案大小": "1.2 MB", "偵測到的檔案類型 (MIME)": "application/zip"}
    timestamp = get_current_taipei_time_str()
    status_icon = "\U00002705" if success else "\U0000274C" # ✅ or ❌
    report_lines = []
    report_lines.append(f"[{timestamp}] [{step_number}/{total_steps}] {step_name}...")

    if details:
        if isinstance(details, dict):
            for key, value in details.items():
                report_lines.append(f"      - {key}: {value}")
        elif isinstance(details, list):
            for item in details:
                report_lines.append(f"      - {item}")

    report_lines.append(f"      {status_icon} {message}")
    report_lines.append("") # Add a blank line for readability

    # Log and print
    for line in report_lines: logger.info(line)
    return "\n".join(report_lines)

def generate_final_report_banner(original_filename, success):
    # 產生處理結束的橫幅報告
    timestamp = get_current_taipei_time_str()
    status_message = "已處理完畢" if success else "處理失敗或部分成功"
    status_icon = "\U0001F389" if success else "\U0001F6A8" # 🎉 or 🚨
    report_lines = []
    report_lines.append("-"*60)
    report_lines.append(f"[{timestamp}] {status_icon} 上傳項目 {original_filename} {status_message}！")
    report_lines.append("="*60)
    # Log and print
    for line in report_lines: logger.info(line)
    return "\n".join(report_lines)

def generate_summary_report(overall_status_message, file_summaries):
    # 產生一個包含所有已處理檔案摘要的總報告 (類似Taifexdtool.py中的generate_summary_report)
    # Args:
    #   overall_status_message (str): 整體處理狀態的訊息
    #   file_summaries (list[dict]): 每個檔案的處理摘要列表
    #                                 每個dict應包含如 "source_filename", "status", "message", "details" 等鍵
    timestamp = get_current_taipei_time_str()
    report_lines = []
    report_lines.append("")
    report_lines.append("="*70)
    report_lines.append(f"[{timestamp}] === 資料處理總結報告 ===")
    report_lines.append("="*70)
    report_lines.append(f"整體狀態: {overall_status_message}")
    report_lines.append("")

    if not file_summaries:
        report_lines.append("沒有處理任何檔案。")
    else:
        total_files = len(file_summaries)
        successful_files = sum(1 for summary in file_summaries if summary.get("status") == "成功")
        failed_files = total_files - successful_files

        report_lines.append(f"總共處理檔案數: {total_files}")
        report_lines.append(f"成功處理檔案數: {successful_files}")
        report_lines.append(f"處理失敗檔案數: {failed_files}")
        report_lines.append("-"*70)

        for idx, summary in enumerate(file_summaries):
            report_lines.append(f'檔案 {idx + 1}: {summary.get("source_filename", "未知檔案")}')
            report_lines.append(f'  狀態: {summary.get("status", "未知")}')
            if summary.get("message"):
                report_lines.append(f'  訊息: {summary.get("message")}')
            if summary.get("details"): # details可以是字串或列表
                if isinstance(summary["details"], list):
                    for detail_line in summary["details"]:
                        report_lines.append(f"    {detail_line}")
                else:
                    report_lines.append(f'  詳細: {summary.get("details")}')
            report_lines.append("-" * 30)

    report_lines.append("="*70)
    report_lines.append(f"[{timestamp}] === 報告結束 ===")
    report_lines.append("="*70)

    # Log and print
    full_report_str = "\n".join(report_lines)
    logger.info(full_report_str) # 記錄完整報告到日誌
    return full_report_str # 返回字串供顯示

# 簡單測試 (主要在 orchestrator 中整合測試)
# if __name__ == "__main__":
#     logger.info("開始 reporter.py 的 __main__ 測試區塊")
#     print(generate_initial_report_banner("my_data_file.zip", 1200000))
#     print(generate_step_report(1, 4, "檔案內容智慧型識別", True, "已將檔案識別為 ZIP 壓縮檔", {"偵測到的檔案類型 (MIME)": "application/zip"}))
#     print(generate_step_report(2, 4, "內容提取與解壓", True, "檔案已解壓縮 發現 1 個 CSV 檔案 (Daily_2025_06_10.csv)"))
#     print(generate_step_report(3, 4, "格式分析與範本匹配", False, "無法匹配任何已知範本", {"檔案名稱": "unknown_file.dat"}))
#     print(generate_final_report_banner("my_data_file.zip", False))
#     print("\n\n--- 總結報告測試 ---")
#     summaries = [
#         {"source_filename": "file1.zip", "status": "成功", "message": "處理完成", "details": ["解壓出 2 個檔案", "資料已儲存"]},
#         {"source_filename": "file2.dat", "status": "失敗", "message": "檔案類型不支援", "details": "MIME: application/octet-stream"},
#         {"source_filename": "file3.csv", "status": "成功", "message": "處理完成", "details": "資料已儲存"}
#     ]
#     print(generate_summary_report("部分檔案處理成功", summaries))
