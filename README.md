# Apache Airflow on Docker 🪂

以 `docker compose` 部署的 [Apache Airflow](https://airflow.apache.org/) 專案，用於排程執行財務、採購、IT 相關的自動化流程，整合 **SAP RFC**、**MS SQL Server（BPM）**、**Selenium 網頁爬蟲**，並以 Email 寄送執行結果與 Excel 附件。

---

## 🏗️ 架構概覽

| 元件 | 說明 |
| --- | --- |
| Apache Airflow | 2.9.0（Base image：`apache/airflow:2.9.0-python3.9`） |
| Python | 3.9 |
| Executor | **LocalExecutor**（`AIRFLOW__CORE__PARALLELISM=8`） |
| Metadata DB | PostgreSQL 13（資料保存在 `postgres-data/`） |
| Reverse Proxy | Nginx 1.27（對外開放 port 80，轉發至 `airflow-webserver:8080`） |
| 時區 | 所有 DAG 皆以 `Asia/Taipei` 設定排程 |

### Docker Compose 服務

| Service | 用途 |
| --- | --- |
| `postgres` | Airflow metadata 資料庫 |
| `airflow-init` | 初始化：建立目錄、修正權限、升級 DB、建立 Web 管理者帳號 |
| `airflow-webserver` | Airflow UI（不直接對外，只透過 Nginx 存取） |
| `airflow-scheduler` | 排程器，同時以 LocalExecutor 執行 task（`mem_limit: 6g`） |
| `airflow-cli` | 除錯用，僅在 `--profile debug` 時啟動 |
| `nginx` | 反向代理，掛載 `nginx/default.conf` |

### 自訂 Image（`Dockerfile`）

* **系統套件**：Google Chrome（stable）、`xvfb`、MS ODBC Driver 17（`msodbcsql17`）、`unixodbc-dev`、gcc/g++ 等編譯工具
* **SAP NW RFC SDK**：由 `plugins/nwrfcsdk/` 複製至 `/opt/nwrfcsdk`，並設定 `SAPNWRFC_HOME`、`LD_LIBRARY_PATH`
* **Python 套件**：`pyrfc==3.3.1`、`pyodbc`、`selenium`、`webdriver-manager`、`pandas`、`openpyxl`、`requests`、`python-dotenv`

---

## 📁 目錄結構

```
.
├── dags/                                   # DAG 定義（依部門分類）
│   ├── .env                                # 業務連線設定（DB / SAP），不進版控
│   ├── IT/
│   │   ├── BPM/bpm_member_data_compare.py
│   │   └── utils/{check_connetions.py, clean_old_logs.py}
│   ├── finance/
│   │   ├── bank_data_checker/bank_data_checker.py
│   │   └── fx_rate/fx_{common,rare}_to_sap_{m,v}.py
│   └── procurement/
│       └── supplier_assessment/{supplier_assessment_pending_list_loader.py, supplier_compliance_crawler.py}
├── plugins/
│   ├── nwrfcsdk/                           # SAP NetWeaver RFC SDK
│   └── src/                                # 業務邏輯（DAG 透過 `from src.xxx import ...` 引用）
│       ├── common/                         # 共用：連線字串、連線檢查、Email 通知
│       ├── bank_data_checker/              # 銀行代碼比對
│       ├── bpm_member_data_compare/        # BPM 人員資料比對
│       ├── fx_to_sap/                      # 匯率爬取 / 轉換 / 寫入 SAP 與 BPM
│       └── supplier_assessment_tasks/      # 供應商考核清單與法遵爬蟲
├── nginx/                                  # 不進版控
│   ├── default.conf                        # Nginx vhost 設定
│   └── ssl/                                # 憑證存放處（選用）
├── downloads/                              # 爬蟲下載的暫存檔（不進版控）
├── export/                                 # 產出的 Excel 附件（不進版控）
├── logs/                                   # Airflow logs（不進版控）
├── postgres-data/                          # PostgreSQL 資料（不進版控）
├── .env                                    # Airflow / SMTP 設定，不進版控
├── docker-compose.yaml
├── Dockerfile
└── README.md
```

---

## 📋 DAG 清單

| DAG ID | 排程（Asia/Taipei） | 說明 |
| --- | --- | --- |
| `fx_common_to_sap_m` | 每月 7、17、27 日 09:30 | 爬取關港貿單一窗口「每旬報關適用外幣匯率」，換算成目標幣別後寫入 SAP（Rate type **M**），並同步寫入 BPM 匯率表 |
| `fx_common_to_sap_v` | 每月 17 日 09:30 | 同上，寫入 SAP（Rate type **V**，生效日為月底），不寫 BPM |
| `fx_rare_to_sap_m` | 每月 1、11、21 日 09:30 | 由 OANDA 爬取特殊幣別匯率，寫入 SAP（Rate type **M**）與 BPM |
| `fx_rare_to_sap_v` | 每月 21 日 09:30 | 同上，寫入 SAP（Rate type **V**） |
| `bank_data_checker` | 每月 10、20 日 09:00 | 比對 SAP 供應商銀行代碼與銀行局、財金資訊公司公開清單，找出 SAP 有但官方清單沒有的代碼 |
| `bpm_member_data_compare` | 每週一 12:00 | 比對 BPM UAT 與 PRD 的人員組織資料，產出新增 / 刪除 / 更新差異表 |
| `supplier_assessment_pending_list_loader` | 每年 7/1 12:10 | 由 SAP 取得近年度收貨（GR）資料，換算台幣、排除去年已考核廠商，產生待考核供應商清單與爬蟲清單 |
| `supplier_compliance_crawler` | 每年 7/1 12:30 | 依爬蟲清單查詢供應商法遵資料（勞動部、環境資訊、職安署），多執行緒爬取後寫入 TMP → HIS / PRD 資料表 |
| `clean_airflow_old_logs` | 每日 08:00 | 刪除超過保留天數的 Airflow log 與空資料夾 |
| `check_connetions` | 每日 08:00 | 檢查 Selenium、MSSQL、SAP RFC 連線是否正常 |

> 新 DAG 預設為暫停狀態（`DAGS_ARE_PAUSED_AT_CREATION=true`），部署後需在 UI 手動啟用。

### 共通設計

* **防止啟用時補跑**：所有 DAG 開頭都有 `on_schedule_check`（`ShortCircuitOperator`）。Airflow 在啟用 DAG 時會立即補跑最近一個已結束的排程區間；若排程 run 比預定時間晚超過 60 分鐘，即視為補跑並 skip 所有後續 task（不寫入資料、不寄信）。手動觸發不受影響。
* **連線檢查**：多數 DAG 起始會先執行 `check_selenium_connection` / `check_mssql_connection` / `check_rfc_connection`，失敗即中止。
* **Task 間資料傳遞**：使用 XCom，DataFrame 一律轉成 `to_dict("records")` 傳遞。
* **Email 通知**（`plugins/src/common/email_utils.py`）：
  * `on_failure_callback`：任一 task 失敗即寄送失敗通知。
  * `end` task 的 `on_success_callback`：寄送成功通知，並自動附上 `gen_attchments` task 回傳的 Excel 檔（存於 `export/`）。
  * 收件者 = Airflow Variable `email_receiver_list` + DAG `params.additional_email_receiver_list` 指向的 Variable。

---

## 🚀 安裝與部署

### 0. 前置需求

* Git
* Docker & Docker Compose plugin
* ufw（或其他防火牆工具）

### 1. 取得原始碼

```bash
git clone https://github.com/a14506818/airflow-docker-pec.git
cd airflow-docker-pec
git checkout <branch-name>
```

### 2. 設定 `.env`

> ⚠️ 兩個 `.env` 都包含帳號密碼，已列在 `.gitignore`，**請勿提交至版控**，並建議設定 `chmod 600`。

#### 根目錄 `.env`（Airflow / SMTP）

```bash
AIRFLOW_UID=<執行 `id -u` 取得>
_AIRFLOW_WWW_USER_USERNAME=<web-admin-username>
_AIRFLOW_WWW_USER_PASSWORD=<web-admin-password>

AIRFLOW__WEBSERVER__SECRET_KEY=<random-secret>

# SMTP Server
AIRFLOW__SMTP__SMTP_HOST=<smtp-host>
AIRFLOW__SMTP__SMTP_PORT=<smtp-port>
AIRFLOW__SMTP__SMTP_STARTTLS=False
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=<smtp-user>
AIRFLOW__SMTP__SMTP_PASSWORD=<smtp-password>
AIRFLOW__SMTP__SMTP_MAIL_FROM=<sender@example.com>
```

`AIRFLOW__WEBSERVER__SECRET_KEY` 可用以下指令產生：

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

#### `dags/.env`（業務連線，由 `plugins/src/common/common.py` 載入）

```bash
# 預設 DB（多數 DAG 使用；測試機請指向 UAT）
DB_DRIVER="ODBC Driver 17 for SQL Server"
DB_SERVER=<db-server>
DB_DATABASE=<db-name>
DB_USERNAME=<db-user>
DB_PASSWORD=<db-password>

# UAT DB（bpm_member_data_compare 使用）
UAT_DB_DRIVER="ODBC Driver 17 for SQL Server"
UAT_DB_SERVER=<uat-db-server>
UAT_DB_DATABASE=<uat-db-name>
UAT_DB_USERNAME=<uat-db-user>
UAT_DB_PASSWORD=<uat-db-password>

# PRD DB（bpm_member_data_compare 使用；測試機建議留空，避免誤連正式資料庫）
PRD_DB_DRIVER="ODBC Driver 17 for SQL Server"
PRD_DB_SERVER=<prd-db-server>
PRD_DB_DATABASE=<prd-db-name>
PRD_DB_USERNAME=<prd-db-user>
PRD_DB_PASSWORD=<prd-db-password>

# SAP RFC
SAP_USER=<sap-user>
SAP_PASS=<sap-password>
SAP_ASHOST=<sap-app-server>
SAP_SYSNR=<system-number>
SAP_CLIENT=<client>
SAP_LANG=EN
```

### 3. 設定 Nginx

```bash
mkdir -p nginx
vi nginx/default.conf
```

```nginx
server {
    listen 80;
    server_name <your-domain-name>;

    location / {
        proxy_pass http://airflow-webserver:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

> 目前 `docker-compose.yaml` 只開放 port **80**。若要啟用 HTTPS，需將憑證放在 `nginx/ssl/`、在 compose 中掛載該目錄並加開 `443:443`，再於 `default.conf` 新增 `listen 443 ssl` 區塊。

### 4. 設定權限

```bash
sudo chown -R $USER:$USER ./

# 讓目前使用者不需 sudo 即可執行 docker
sudo usermod -aG docker $USER
newgrp docker   # 或登出後重新登入
```

### 5. Build 與啟動

```bash
docker compose build              # 首次或修改 Dockerfile 後執行；可加 --no-cache
docker compose up airflow-init    # 初始化 DB 與管理者帳號
docker compose up -d              # 啟動所有服務
```

### 6. 設定 Airflow Variables

在 UI 的 **Admin → Variables** 新增以下變數（值皆為 JSON，除 `log_retention_days` 外）：

| Variable | 格式 | 使用者 |
| --- | --- | --- |
| `email_receiver_list` | `["a@example.com", "b@example.com"]` | 所有 DAG 的成功 / 失敗通知 |
| `fx_rate_ERL` | `["finance@example.com"]` | 匯率 DAG 額外收件者 |
| `bank_data_checker_ERL` | `["finance@example.com"]` | `bank_data_checker` 額外收件者 |
| `fx_target_currency_list` | `["TWD", "USD"]` | 匯率要換算的目標幣別 |
| `fx_rare_currency_list` | `["XXX", "YYY"]` | OANDA 爬取的特殊幣別 |
| `log_retention_days` | `30` | log 保留天數（預設 30） |

### 7. 存取 Airflow UI

* **URL**：`http://<your-domain-name>`（或 `http://<host-ip>`），經由 Nginx 轉發。
* webserver 的 8080 port 未對主機開放，請一律透過 Nginx 存取。
* 登入帳號密碼為根目錄 `.env` 的 `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`（未設定時預設為 `airflow` / `airflow`，**正式環境務必修改**）。

### 8. 防火牆

```bash
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp   # 有啟用 HTTPS 時
```

若部署於雲端，請一併調整 Security Group / 防火牆規則。

### 9. 驗證

在 UI 手動觸發 `check_connetions`，確認 Selenium、MSSQL、SAP RFC 三項連線皆成功。

---

## 🛠️ 常用指令

| 指令 | 說明 |
| --- | --- |
| `docker compose ps` | 查看容器狀態 |
| `docker compose logs -f airflow-scheduler` | 追蹤 scheduler log |
| `docker compose restart` | 重啟服務（修改 `.env` 或 plugins 後） |
| `docker compose build && docker compose up -d` | 修改 `Dockerfile` 後重建 |
| `docker compose down` | 停止並移除容器 |
| `docker compose exec airflow-scheduler bash` | 進入 scheduler 容器 |
| `docker compose --profile debug run --rm airflow-cli <command>` | 執行 Airflow CLI 指令 |

---

## 🧑‍💻 開發說明

* **新增 DAG**：放在 `dags/<部門>/<功能>/`，DAG 檔只負責定義流程，業務邏輯放在 `plugins/src/<功能>/`。
* **引用模組**：`plugins/` 會自動加入 `PYTHONPATH`，使用 `from src.<module> import ...`。
* **寄送附件**：讓最後一個業務 task 的 `task_id` 為 `gen_attchments` 並回傳檔案路徑 list，再接到掛有 `on_success` 的 `end` task。
* **連線設定**：DB / SAP 連線一律透過 `src.common.common` 的 `get_mssql_conn_str()`、`get_sap_conn_params()` 等函式取得，不要在程式碼中寫死帳密。
* **檔案路徑**：爬蟲下載使用 `/opt/airflow/downloads`，產出檔使用 `/opt/airflow/export`。
* **Log 清理**：log 由 `clean_airflow_old_logs` 每日自動清理，保留天數可透過 Variable 調整。

---

## 🔐 安全注意事項

* `.env`、`dags/.env`、`nginx/default.conf`、`postgres-data/`、`logs/`、`export/`、`downloads/` 皆已列入 `.gitignore`，請勿強制加入版控。
* `export/` 與 `logs/` 可能含有業務資料與連線資訊，請限制主機存取權限。
* 正式環境請修改 Web 管理者預設密碼，並設定 `AIRFLOW__WEBSERVER__SECRET_KEY`。
* 測試機的 `dags/.env` 中 `PRD_*` 建議留空，避免誤寫正式資料庫。

---

## 📄 License

MIT © Justin Yang
