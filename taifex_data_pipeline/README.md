# 期交所資料自動化處理管道 - TAIFEX Data Pipeline

[![在 Colab 中開啟](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/YourGitHubUsername/YourGitHubRepoName/blob/main/taifex_data_pipeline/run_pipeline.ipynb)

## 專案概述

本專案旨在開發一個自動化的資料處理應用程式，其核心部署與執行環境為 Google Colaboratory。主要功能是接收使用者上傳的、格式可能多樣的期交所原始資料檔案，透過**基於檔案內容的智慧型識別機制**準確判斷其真實類型，執行相應的解壓縮與解析流程，最終將結構化資料存入一個 SQLite 資料庫。整個過程對使用者而言應是極度簡化的，並能獲得即時、詳細、完全中文化的處理狀態回饋。

## 主要功能特性

*   **智慧型檔案識別**: 不依賴副檔名，採用基於檔案「魔術數字」(Magic Number) 的內容分析技術，準確識別檔案的真實 MIME 類型。
*   **多元格式支援**: 能夠處理多種常見的壓縮格式（如 ZIP, GZIP, RAR, 7z 等）以及純文字/CSV 檔案。
*   **Google Colab 最適化**: 專為在 Google Colab 環境中執行而設計，包含依賴安裝與資源管理考量。
*   **深度中文化**: 所有使用者介面、日誌輸出、狀態報告及程式碼註解均使用繁體中文。
*   **結構化資料儲存**: 將解析後的資料儲存於 SQLite 資料庫中。
*   **簡化操作**: Colab Notebook 設計為單一儲存格執行，簡化使用者操作流程。

## 專案目錄結構

```
taifex_data_pipeline/
├── run_pipeline.ipynb         # 【啟動器】使用者互動的唯一入口 (Colab Notebook)
├── README.md                  # 中文專案說明文件
├── requirements.txt           # Python 依賴庫
└── src/                       # 【核心邏輯區】
    ├── config/                # 設定中心
    │   ├── settings.py        # 各項設定值
    │   └── templates.py       # (未來擴展用) 資料格式範本
    │
    ├── pipeline/              # 核心處理管道
    │   ├── file_handler.py    # 智慧型檔案處理器 (MIME偵測、解壓縮)
    │   ├── parser.py          # 資料剖析器 (CSV剖析)
    │   └── database.py        # 資料庫管理器 (SQLite操作)
    │
    ├── orchestrator.py        # 總指揮 (協調處理流程)
    └── utils/                 # 工具程式
        ├── logger.py          # 日誌記錄器
        └── reporter.py        # 中文報告產生器
```

## 使用方法

1.  **開啟 Colab Notebook**:
    *   點擊本文件上方的 "Open in Colab" 徽章，或直接將 `run_pipeline.ipynb` 上傳到您的 Google Colab 環境中開啟。
2.  **上傳專案資料夾**:
    *   確保 `taifex_data_pipeline` 整個資料夾 (包含 `src` 子目錄, `requirements.txt` 等) 已上傳到您的 Google Drive，或者與 `run_pipeline.ipynb` 同時上傳到 Colab 的臨時儲存空間，並保持相對路徑正確。
    *   若 Notebook 是從 GitHub 開啟，Colab 通常會將整個倉庫複製到執行環境，此時路徑應已正確。
3.  **執行儲存格**:
    *   執行 Notebook 中的唯一一個程式碼儲存格。
    *   儲存格會自動處理：
        *   安裝必要的系統套件 (如 `libmagic1`, `unrar`, `p7zip-full`)。
        *   安裝 `requirements.txt` 中列出的 Python 套件。
        *   提示您上傳期交所的資料檔案。
        *   執行完整的資料處理流程。
        *   在儲存格下方輸出詳細的中文處理日誌與報告。
4.  **獲取結果**:
    *   處理完成後，結構化的資料將儲存在與 `run_pipeline.ipynb` 同目錄 (或 `taifex_data_pipeline` 目錄下，取決於執行上下文) 的 `taifex_data.sqlite` 資料庫檔案中。
    *   詳細的執行日誌也會儲存在 `taifex_pipeline.log` 檔案中。

## 依賴需求

### Python 套件
詳細列表請見 `requirements.txt`，主要包含：
*   `python-magic` (用於檔案類型偵測)
*   `patool` (用於處理多種壓縮格式)
*   `pytz` (用於時區處理)

### 系統套件 (Colab 環境中會自動安裝)
*   `libmagic1`
*   `unrar` 或 `unrar-free`
*   `p7zip-full`

## 注意事項

*   **首次執行**: Colab 環境初次設定和安裝依賴可能需要一些時間。
*   **檔案路徑**: 請確保 `run_pipeline.ipynb` 能夠正確找到 `src` 目錄以及 `requirements.txt`。若從 GitHub 開啟，通常路徑會自動配置正確。若手動上傳，建議將整個 `taifex_data_pipeline` 資料夾上傳至 Colab 工作階段的根目錄 (`/content/`)，然後從該目錄內執行 Notebook，或確保 Notebook 能正確引用到 `src`。
*   **資源限制**: 處理非常大的檔案時，請注意 Colab 的記憶體和執行時間限制。
*   **錯誤排除**: 詳細的錯誤訊息會記錄在 `taifex_pipeline.log` 檔案中，以及 Colab 儲存格的輸出區域。
