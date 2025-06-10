#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test.py - CSV 解析策略測試器

此腳本提供一個命令列介面，用以針對給定的 CSV 檔案測試多種解析策略
（定義為「資料識別範本」）。它有助於為未知或多樣化的 CSV 格式
決定合適的解析參數（如分隔符號、編碼、忽略行數等）。

腳本會將每個預定義的範本應用於目標 CSV 檔案，回報使用該範本解析的
成功或失敗狀態，並根據讀取的行數提供最佳匹配範本的基本建議。
"""

import os
import argparse
import csv
# import json
# import logging # 未來可能匯入的模組

# 資料識別範本的預留位置
# 每個字典代表一個針對 CSV 檔案的潛在解析策略。
# 這些參數最終將用於指導或設定解析邏輯（可能透過呼叫
# Taifexdtool.py 中的函數，或在 test.py 中重新實作一個彈性的解析器）。

DATA_RECOGNITION_TEMPLATES = [
    {
        "template_name": "標準逗號分隔 (Standard Comma Separated)",
        "description": "假設為標準 CSV 格式，使用逗號分隔，標頭位於第一行。",
        "parser_params": {
            "delimiter": ",",
            "skiprows": 0, # 或 header_row: 0
            "encoding": "utf-8",
            # 其他潛在參數，如 quotechar, escapechar, column_names, type_hints 等。
        }
    },
    {
        "template_name": "分號分隔帶標頭 (Semicolon Separated with Header)",
        "description": "假設為使用分號分隔的 CSV，標頭位於第一行。",
        "parser_params": {
            "delimiter": ";",
            "skiprows": 0,
            "encoding": "utf-8",
        }
    },
    {
        "template_name": "逗號分隔，略過 2 行 (Comma Separated, Skip 2 Rows)",
        "description": "假設為使用逗號分隔的 CSV，實際資料從略過 2 行後開始。",
        "parser_params": {
            "delimiter": ",",
            "skiprows": 2,
            "encoding": "utf-8",
        }
    },
    {
        "template_name": "UTF-16 編碼，Tab 分隔 (UTF-16 Encoded, Tab Separated)",
        "description": "一個使用不同編碼和分隔符號的更複雜範例。",
        "parser_params": {
            "delimiter": "\t", # Tab 字元
            "skiprows": 0,
            "encoding": "utf-16",
        }
    }
]

def apply_template_to_csv(csv_filepath, template):
    """
    將單一解析範本應用於 CSV 檔案並回報結果。

    它會嘗試使用範本中定義的參數（分隔符號、編碼、忽略行數）來讀取 CSV。
    它會擷取基本的解析結果，包括成功/失敗、讀取的行數、資料範例以及任何錯誤訊息。

    參數:
        csv_filepath (str): 要測試的 CSV 檔案路徑。
        template (dict): 代表解析範本的字典。預期鍵值：
                         'template_name' (str), 
                         'parser_params' (dict，包含 'delimiter', 'encoding', 'skiprows')。

    返回:
        dict: 一個總結套用範本結果的字典：
              - 'template_name' (str): 所使用範本的名稱。
              - 'success' (bool): 如果解析成功（無嚴重錯誤）則為 True，否則為 False。
              - 'rows_read' (int): 略過指定行數後，csv.reader 成功讀取的行數。
              - 'error_message' (str or None): 如果解析失敗則為錯誤訊息，否則為 None。
              - 'sample_data' (list of lists): 如果成功，則為已解析資料的小部分範例（前幾行），
                                             否則為空列表。
    """
    template_name = template.get("template_name", "未知範本")
    parser_params = template.get("parser_params", {})
    
    delimiter = parser_params.get("delimiter", ",")
    encoding = parser_params.get("encoding", "utf-8")
    skiprows = parser_params.get("skiprows", 0)

    # print(f"嘗試使用範本 '{template_name}' (分隔符: '{delimiter}', 編碼: '{encoding}', 略過行數: {skiprows}) 解析 '{csv_filepath}'...")

    result = {
        'template_name': template_name,
        'success': False,
        'rows_read': 0,
        'error_message': None,
        'sample_data': []
    }

    try:
        with open(csv_filepath, 'r', encoding=encoding, newline='') as f:
            # 如果指定，則略過起始行
            for _ in range(skiprows):
                next(f) # 讀取並捨棄標頭/略過的行
            
            reader = csv.reader(f, delimiter=delimiter)
            
            read_count = 0
            for i, row in enumerate(reader):
                if i < 5: # 為範例讀取最多 5 行資料
                    result['sample_data'].append(row)
                read_count += 1
            
            result['rows_read'] = read_count
            result['success'] = True
            # print(f"  使用 '{template_name}' 成功解析。讀取行數: {read_count}。")

    except FileNotFoundError:
        result['error_message'] = f"找不到檔案: {csv_filepath}"
        # print(f"  範本 '{template_name}' 發生錯誤: {result['error_message']}")
    except UnicodeDecodeError as e:
        result['error_message'] = f"編碼錯誤 ({encoding}): {e}"
        # print(f"  範本 '{template_name}' 發生錯誤: {result['error_message']}")
    except csv.Error as e:
        result['error_message'] = f"CSV 解析錯誤: {e}"
        # print(f"  範本 '{template_name}' 發生錯誤: {result['error_message']}")
    except LookupError as e: # 針對無效的編碼名稱
        result['error_message'] = f"無效的編碼名稱 ('{encoding}'): {e}"
        # print(f"  範本 '{template_name}' 發生錯誤: {result['error_message']}")
    except StopIteration: # 處理略過行後檔案為空的情況
        result['error_message'] = f"略過 {skiprows} 行後，檔案 '{csv_filepath}' 中無內容可讀取。"
        # 這是否算成功取決於預期，但目前如果沒有讀取到資料行，我們會標記它
        if skiprows > 0 : result['success'] = True # 如果我們略過了行然後檔案是空的，那沒關係。
        # print(f"  範本 '{template_name}' 注意事項: {result['error_message']}")
    except Exception as e:
        result['error_message'] = f"發生未預期的錯誤: {e}"
        # print(f"  範本 '{template_name}' 發生錯誤: {result['error_message']}")
        
    return result

def run_tests_on_csv(csv_filepath, templates):
    """
    針對單一 CSV 檔案執行所有定義的解析範本。

    參數:
        csv_filepath (str): 要測試的 CSV 檔案路徑。
        templates (list): 範本字典列表 (例如 DATA_RECOGNITION_TEMPLATES)。

    返回:
        list: 結果字典的列表，其中每個字典是
              針對一個範本執行 `apply_template_to_csv` 的輸出。
    """
    print(f"\n正在對檔案進行所有解析測試: {csv_filepath}")
    results = []
    for template in templates:
        test_result = apply_template_to_csv(csv_filepath, template)
        results.append(test_result)
    return results

def generate_test_report(csv_filepath, test_results):
    """
    產生並在控制台印出格式化的測試報告。

    報告包含每個套用範本的結果，以及一個建議區段，
    該區段根據成功解析和讀取的行數建議可能適合的範本。

    參數:
        csv_filepath (str): 已測試的 CSV 檔案路徑。
        test_results (list): 來自 `run_tests_on_csv` 的結果字典列表。
    """
    print(f"\n--- 測試報告: {csv_filepath} ---")

    successful_templates = [] # 儲存成功讀取資料列的範本

    for result in test_results:
        status = "成功" if result['success'] else "失敗"
        print(f"\n範本: {result['template_name']}")
        print(f"  狀態: {status}")

        if result['success']:
            print(f"  讀取行數: {result['rows_read']}")
            if result['sample_data']:
                # 為求簡潔，僅顯示範例資料的第一行。
                # 限制為前 5 個欄位，每個欄位最多 30 個字元。
                sample_row_display = [str(field)[:30] for field in result['sample_data'][0][:5]]
                print(f"  範例資料 (第一行，最多 5 個欄位，每欄位 30 字元): {sample_row_display}")
            elif result['rows_read'] > 0:
                # 如果 sample_data 為空但 rows_read > 0，可能會發生這種情況，
                # 儘管 apply_template_to_csv 的目前邏輯應在 rows_read > 0 時填入 sample_data。
                print("  範例資料: 未詳細擷取 (但已讀取資料列)。")
            else: # rows_read == 0 但 success == True (例如，僅有標頭的 CSV，或略過後為空檔案)
                print("  範例資料: 不存在或未讀取到資料列。")
            
            # 收集成功讀取至少一個資料列的範本以供建議
            if result['rows_read'] > 0: 
                successful_templates.append({
                    "name": result['template_name'],
                    "rows_read": result['rows_read']
                })
        else:
            print(f"  錯誤: {result['error_message']}") # 顯示失敗範本的錯誤訊息
    
    # --- 建議區段 ---
    print("\n--- 建議 ---")
    if successful_templates:
        print("可能適合的範本 (成功解析並讀取 >0 資料列的範本):")
        
        # 依讀取行數降冪排序成功範本，以找出最佳候選
        successful_templates.sort(key=lambda x: x['rows_read'], reverse=True)
        
        for tpl_info in successful_templates:
            print(f"  - {tpl_info['name']} (讀取 {tpl_info['rows_read']} 行)")

        # 強調讀取最多行的範本
        if successful_templates: # 確保列表在過濾後不為空
            max_rows = successful_templates[0]['rows_read']
            # 找出所有達到此最大行數的範本
            best_candidates = [tpl['name'] for tpl in successful_templates if tpl['rows_read'] == max_rows]
            
            if len(best_candidates) == 1:
                print(f"\n依據最多讀取行數的最佳候選範本: {best_candidates[0]} ({max_rows} 行)")
            else: # 多個範本共享最大行數
                print(f"\n最佳候選範本 (均讀取 {max_rows} 資料列):")
                for candidate_name in best_candidates:
                    print(f"  - {candidate_name}")
    else:
        # 如果沒有範本成功且 rows_read > 0，則印出此訊息
        print("找不到成功解析實際資料列的合適範本。")
    
    print("\n--- 報告結束 ---")


def main():
    """
    CSV 解析策略測試器腳本的主要進入點。

    解析命令列參數（預期一個可選的 CSV 檔案路徑），
    顯示已定義的解析範本，如果提供了有效的 CSV 檔案，
    則對其執行所有範本並產生測試報告。
    """
    # 設定命令列參數解析
    parser = argparse.ArgumentParser(description="Taifexdtool CSV 解析與處理策略的測試框架。")
    parser.add_argument(
        "csv_file", 
        nargs='?', # 使參數成為可選
        default=None, # 若未提供參數，則使用預設值
        help="要測試的 CSV 檔案路徑。(可選)"
    )
    # 未來如何加入其他參數的範例：
    # parser.add_argument("-t", "--template", help="指定要單獨執行的解析範本/策略名稱。")
    # parser.add_argument("-v", "--verbose", action="store_true", help="為此腳本啟用詳細 (DEBUG 等級) 輸出。")
    
    args = parser.parse_args()

    print("`test.py` - 期交所資料處理設定測試器 - 初始設定")

    # 顯示關於提供的 CSV 檔案參數的資訊
    if args.csv_file:
        print(f"指定用於測試的 CSV 檔案: {args.csv_file}")
        if not os.path.exists(args.csv_file):
            # 如果使用者打錯路徑，此警告也很有用。
            print(f"警告: 指定的 CSV 檔案不存在: {args.csv_file}")
    else:
        print("此次執行未指定用於測試的 CSV 檔案。")

    # 無論是否有 CSV 輸入，都顯示已定義的範本供使用者參考
    if DATA_RECOGNITION_TEMPLATES:
        print("\n已定義的資料識別範本:")
        for template in DATA_RECOGNITION_TEMPLATES:
            print(f"  - {template['template_name']}: {template['description']}")
        print("-" * 30) #視覺分隔線
    
    # 如果提供了 CSV 檔案路徑且檔案實際存在，則繼續測試
    if args.csv_file:
        if os.path.exists(args.csv_file):
            # 針對指定的 CSV 檔案執行所有已定義的範本
            results = run_tests_on_csv(args.csv_file, DATA_RECOGNITION_TEMPLATES)
            # 根據結果產生並印出格式化的報告
            generate_test_report(args.csv_file, results)
        else:
            # 如果檔案不存在，重申無法執行測試。
            print(f"\n無法執行測試：CSV 檔案 '{args.csv_file}' 不存在。")
    else:
        # 如果命令列未指定 CSV 檔案，則引導使用者。
        print("\n未提供 CSV 檔案。若要執行測試，請指定一個 CSV 檔案路徑作為參數。")


if __name__ == "__main__":
    main()
