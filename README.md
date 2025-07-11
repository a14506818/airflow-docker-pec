# Apache Airflow on Docker 🪂

This repository contains a pre-configured Docker-based setup of [Apache Airflow](https://airflow.apache.org/) using `docker-compose`. It is suitable for development, testing, and deployment of DAGs.

## 🏗️ Stack Overview

This Airflow project is containerized with Docker and designed for running complex DAGs with SAP integration, Excel/CSV processing, browser automation, and SQL Server access.

- **Apache Airflow** `2.9.0`
- **Python** `3.9`
- **Executor**: `CeleryExecutor`
- **Database**: `PostgreSQL 13`
- **Broker**: `Redis`
- **Base Image**: `apache/airflow:2.9.0-python3.9`
- **Custom Additions**:
  - ✅ `pyrfc` (SAP RFC)
  - ✅ `openpyxl`, `pandas` (Excel/CSV processing)
  - ✅ `selenium`, `webdriver-manager`, `xvfb` (Headless Chrome automation)
  - ✅ `pyodbc`, `msodbcsql17` (SQL Server driver for ODBC)
  - ✅ `python-dotenv` (env var management)
- **Plugin Folders**:
  - `plugins/src/`: custom Python logic
  - `plugins/nwrfcsdk/`: SAP RFC SDK
- **Custom Dockerfile**: builds image with all above dependencies
- **Volume Mapping**:
  - `dags/`, `plugins/`, `logs/`, `downloads/`, `export/`
- **Startup**: Requires `docker compose build` before `up -d` (due to custom image)

## 📁 Folder Structure

```
.
├── dags/                        # Your DAG files
│   └── .env                     # DAG-level connection settings 
├── logs/                        # Airflow logs (auto-generated)
├── plugins/                     # Custom plugins
│   ├── src/                     # Custom Python modules (your business logic)
│   └── nwrfcsdk/                # SAP NetWeaver RFC SDK library (for pyrfc)
├── downloads/                   # Folder for temporary or input files
├── export/                      # Folder for exported reports/files
├── .env                         # Global environment variables (AIRFLOW_UID, SMTP, etc.)
├── docker-compose.yaml          # Docker Compose setup for Airflow
├── Dockerfile                   # Custom Dockerfile (e.g., installing pyrfc, openpyxl)
└── README.md                    # Project documentation

```

## 🚀 Getting Started
### 0. Installation

##### a. Git
##### b. Docker

### 1. Clone the repository

```bash
git clone https://github.com/a14506818/airflow-docker-pec.git
cd airflow-docker-pec

git checkout <branch-name>
```

### 2. Set .env files

There are two .env files, one in root folder another in dags folder.

root:
```bash
AIRFLOW_UID=<run 'id -u'>

AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp
AIRFLOW__SMTP__SMTP_HOST=<your host IP>
AIRFLOW__SMTP__SMTP_PORT=<your host port>
AIRFLOW__SMTP__SMTP_STARTTLS=False
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_MAIL_FROM=Airflow_UAT@PharmaEssentia.com
```

dags:
```bash
DB_DRIVER=ODBC Driver 17 for SQL Server
DB_SERVER=<your server IP>
DB_DATABASE=<your DB name>
DB_USERNAME=<your user>
DB_PASSWORD=<your pwd>

SAP_USER=<your user>
SAP_PASS=<your pwd>
SAP_ASHOST=<your host IP>
SAP_SYSNR=00
SAP_CLIENT=<your client>
SAP_LANG=EN
```

### 3. Set Project Permission
```bash
sudo chown -R $USER:$USER ./
```

### 4. Initialize Airflow
```bash
docker compose up airflow-init
```

### 5. Start the Airflow services

```bash
docker compose build --no-cache
docker compose up -d
```

```bash
# First time
docker compose build --no-cache
docker compose up -d

# Update Dads
docker compose restart

# Update Dockerfile
docker compose build
docker compose up -d
```

### 6. Access the Airflow UI

- URL: [http://localhost:8080](http://localhost:8080)
- URL: [http://ip_address:8080](http://ip_address:8080)
- Default login:
  - **Username:** `airflow`
  - **Password:** `airflow`

Allow Others to Connent server:
```bash
# close firewall
sudo iptables -L -n | grep 8080
```

---

## 🛠 Common Commands

| Command | Description |
|--------|-------------|
| `docker compose ps` | View running containers |
| `docker compose logs -f` | Follow logs |
| `docker compose down` | Stop all services |
| `docker compose up -d` | Start services in background |
| `docker compose exec airflow-webserver bash` | Shell into container |

---

## ⚠️ Notes

- All DAGs must be placed in the `dags/` folder.
- Make sure to restart services after modifying `docker-compose.yaml` or `.env`.
- Use `XCom` or Airflow Variables to pass values between tasks.
- Avoid writing heavy logic directly in DAG files—use functions.

---

## 📄 License

MIT © [Justin Yang]
