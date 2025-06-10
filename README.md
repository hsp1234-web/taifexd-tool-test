# 期交所資料處理工具

[![在 Colab 中開啟](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/[在此填寫您的 GitHub 使用者名稱]/[在此填寫您的 GitHub 倉庫名稱]/blob/main/taifex_data_pipeline/run_pipeline.ipynb)

本專案包含用於下載、處理及測試資料的 Python 指令稿，最初設計時以臺灣期貨交易所 (TAIFEX) 的資料為主要處理對象。專案內亦包含一個名為 `taifex_data_pipeline` 的子專案，提供更進階的自動化資料處理流程。

## 指令稿

### 1. `Taifexdtool.py`

**目的：** 自動化處理資料檔案（例如來自期交所的 CSV 檔案）。其功能包括：
- 從 `config.json` 載入設定。
- 「下載」資料（目前從本地檔案模擬，可擴展以進行網路下載）。
- 解析 CSV 檔案，包括基本清理（去除多餘空白）。
- 用於未來資料類型識別與資料轉換的預留函數。
- 將處理後的資料（以 JSON 字串格式）儲存至 SQLite 資料庫（預設為 `taifex_data.sqlite`）。
- 將所有操作記錄到設定的日誌檔案（例如 `taifexdtool.log`）並輸出到控制台。
- 產生本次執行所處理之所有檔案的摘要報告。

**基本用法：**
```bash
python3 Taifexdtool.py
```
設定透過 `config.json` 管理。指令稿會處理 `config.json` 中 `download_urls` 陣列所列出的資料來源。若此陣列為空或不存在，則會使用預設的測試來源列表（本地檔案與範例網址）。

### 2. `test.py`

**目的：** 一個命令列工具，用於針對特定的 CSV 檔案測試和評估不同的解析策略（「資料識別範本」）。這有助於為 `Taifexdtool.py` 找出最佳參數，或理解新的或多樣化的 CSV 格式結構。

**功能特性：**
- 定義一組解析範本（例如，不同的分隔符、編碼、忽略行數）。
- 將每個定義的範本應用於使用者指定的目標 CSV 檔案。
- 回報每個範本成功（檔案讀取、基本解析）或失敗的狀態。
- 根據成功的解析與讀取的行數，提供最適用範本的基本建議。

**基本用法：**
```bash
python3 test.py <要測試的CSV檔案路徑>
```
範例：
```bash
python3 test.py sample_data.csv
```
若未提供 CSV 檔案路徑，則會印出已定義的範本及使用說明。

## 子專案：`taifex_data_pipeline`

此子專案 (`taifex_data_pipeline/`) 提供一個更為完善的自動化資料處理流程，特別為在 Google Colaboratory 環境中執行而設計。其核心功能是接收使用者上傳的、格式可能多樣的期交所原始資料檔案，透過**基於檔案內容的智慧型識別機制**準確 판단其真實類型，執行相應的解壓縮與解析流程，最終將結構化資料存入 SQLite 資料庫。

主要特色包含：
*   **智慧型檔案識別**: 不依賴副檔名，採用基於檔案「魔術數字」(Magic Number) 的內容分析技術。
*   **多元格式支援**: 能夠處理多種常見的壓縮格式以及純文字/CSV 檔案。
*   **Google Colab 最適化**: 專為在 Google Colab 環境中執行而設計。
*   **深度中文化**: 所有使用者介面、日誌輸出、狀態報告均使用繁體中文。

詳細資訊請參閱 [`taifex_data_pipeline/README.md`](taifex_data_pipeline/README.md)。

## 安裝與依賴

- Python 3.x
- 目前的基本功能除了 Python 標準模組（`argparse`, `csv`, `json`, `logging`, `os`, `sqlite3`）外，不嚴格要求其他外部函式庫。
- `taifex_data_pipeline` 子專案有其自身的依賴需求，詳見其 `requirements.txt`。

## 單元測試
單元測試位於 `tests/` 目錄下。可使用以下指令執行：
```bash
python3 -m unittest discover tests
```
這將執行 `Taifexdtool.py` 的設定載入、日誌設定，以及 `test.py` 的 CSV 解析邏輯測試。

## 未來發展
- 在 `Taifexdtool.py` 中實作真正的網路下載功能。
- 增強 `Taifexdtool.py` 中的資料類型識別能力，以辨識特定的期交所 CSV 結構。
- 根據識別出的資料類型，在 `Taifexdtool.py` 中實作資料轉換邏輯。
- 擴展解析能力以處理更複雜的 CSV（例如，處理特定的期交所頁首/頁尾）及其他潛在格式（例如，包含 CSV 的 ZIP 壓縮檔）。
- 在 `test.py` 中開發更精密的評估指標與報告機制。
- 新增全面的整合測試。
- 改進兩個指令稿的錯誤處理與使用者回饋。
