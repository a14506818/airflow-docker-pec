# Apache Airflow on Docker 🪂

This repository contains a pre-configured Docker-based setup of [Apache Airflow](https://airflow.apache.org/) using `docker-compose`. It is suitable for development, testing, and deployment of DAGs with SAP integration, Excel/CSV processing, browser automation, and SQL Server access.

## 🏗️ Stack Overview

This Airflow project is containerized with Docker and designed for running complex DAGs using CeleryExecutor. Additional services include Redis as the broker, PostgreSQL as metadata DB, and Nginx as a reverse proxy.

* **Apache Airflow**: 2.9.0
* **Python**: 3.9
* **Executor**: CeleryExecutor
* **Metadata Database**: PostgreSQL 13 (persistent in `postgres-data/`)
* **Broker**: Redis
* **Reverse Proxy**: Nginx
* **Base Image**: `apache/airflow:2.9.0-python3.9`
* **Custom Additions**:

  * `pyrfc` (SAP RFC)
  * `openpyxl`, `pandas` (Excel/CSV processing)
  * `selenium`, `webdriver-manager`, `xvfb` (Headless Chrome automation)
  * `pyodbc`, `msodbcsql17` (SQL Server driver)
  * `python-dotenv` (env var management)
* **Plugin Folders**:

  * `plugins/src/`: custom Python logic
  * `plugins/nwrfcsdk/`: SAP NetWeaver RFC SDK
* **Custom Dockerfile**: builds image with all dependencies
* **Volume Mapping**:

  * `dags/`, `plugins/`, `logs/`, `downloads/`, `export/`, `nginx/`, `postgres-data/`

## 📁 Folder Structure

```
.
├── dags/                        # Your DAG files
│   └── .env                     # DAG-level connection settings
├── downloads/                   # Temporary or input files
├── export/                      # Exported reports/files
├── logs/                        # Airflow logs (auto-generated)
├── plugins/                     # Custom plugins
│   ├── src/                     # Custom Python modules
│   └── nwrfcsdk/                # SAP NetWeaver RFC SDK
├── nginx/                       # Nginx configuration for reverse proxy
│   └── default.conf             # Example vhost config
├── postgres-data/               # Persistent PostgreSQL data volume
├── .env                         # Global environment variables
├── docker-compose.yaml          # Docker Compose setup for Airflow + Redis + Postgres + Nginx
├── Dockerfile                   # Custom image to install Python dependencies
└── README.md                    # This documentation
```

## 🚀 Getting Started

### 0. Prerequisites

* Install Git
* Install Docker & Docker Compose

### 1. Clone the repository

```bash
git clone https://github.com/a14506818/airflow-docker-pec.git
cd airflow-docker-pec
git checkout <branch-name>
```

### 2. Configure `.env` files

#### Root `.env`

```bash
AIRFLOW_UID=$(id -u)
AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp
AIRFLOW__SMTP__SMTP_HOST=<your-smtp-host>
AIRFLOW__SMTP__SMTP_PORT=<your-smtp-port>
AIRFLOW__SMTP__SMTP_STARTTLS=False
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_MAIL_FROM=airflow@example.com
```

#### `dags/.env`

```bash
# Default (Production) DB
DB_DRIVER="ODBC Driver 17 for SQL Server"
DB_SERVER=<PROD_DB_SERVER>
DB_DATABASE=<PROD_DB_NAME>
DB_USERNAME=<PROD_DB_USER>
DB_PASSWORD=<PROD_DB_PASSWORD>

# SAP System for BPM
SAP_USER=<SAP_USER>
SAP_PASS=<SAP_PASSWORD>
SAP_ASHOST=<SAP_ASHOST>
SAP_SYSNR=<SAP_SYSNR>
SAP_CLIENT=<SAP_CLIENT>
SAP_LANG=<SAP_LANG>

# UAT Database
UAT_DB_DRIVER="ODBC Driver 17 for SQL Server"
UAT_DB_SERVER=<UAT_DB_SERVER>
UAT_DB_DATABASE=<UAT_DB_NAME>
UAT_DB_USERNAME=<UAT_DB_USER>
UAT_DB_PASSWORD=<UAT_DB_PASSWORD>

# Production Database (Optional override)
PRD_DB_DRIVER="ODBC Driver 17 for SQL Server"
PRD_DB_SERVER=<PRD_DB_SERVER>
PRD_DB_DATABASE=<PRD_DB_NAME>
PRD_DB_USERNAME=<PRD_DB_USER>
PRD_DB_PASSWORD=<PRD_DB_PASSWORD>
```

### 3. Set Permissions

```bash
sudo chown -R $USER:$USER ./
```

### 4. Initialize Airflow

```bash
docker compose up airflow-init
```

### 5. Build and Start Services

```bash
docker compose build --no-cache
docker compose up -d
```

**Tips:**

* To update DAGs: `docker compose restart`
* To rebuild image after updating `Dockerfile`:

```bash
docker compose build
docker compose up -d
```

### 6. Access the Airflow UI

* **URL (behind Nginx):** `https://airflow-test.pharmaessentia.com`

  * The domain is routed through the `nginx/` reverse‑proxy container. Adjust DNS or `/etc/hosts` as needed.
  * TLS certificates should be placed in `nginx/ssl/` (or via an automated Let’s Encrypt flow) and referenced in `default.conf`.
* **Local fallback:** `http://localhost:8080` or `http://<host-ip>:8080`
* **Default login:** `airflow` / `airflow`

### 7. Configure Firewall Settings

```bash
# Allow HTTP and HTTPS traffic (adjust ports as needed)
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# If using a cloud provider, update security group or firewall rules accordingly.
```

Make sure ports **80/443** (or your exposed ports) are open on the host if you need external access.

---

## 🛠️ Common Commands

| Command                                      | Description                        |
| -------------------------------------------- | ---------------------------------- |
| `docker compose ps`                          | View running containers            |
| `docker compose logs -f`                     | Follow all logs                    |
| `docker compose down`                        | Stop and remove containers         |
| `docker compose up -d`                       | Start services in detached mode    |
| `docker compose exec airflow-webserver bash` | Shell into the webserver container |

---

## ⚠️ Notes

* Place all DAG files in `dags/`.
* Restart services after updating `docker-compose.yaml`, `Dockerfile`, or `.env`.
* Use XComs or Airflow Variables for inter-task communication.
* Keep DAG files lightweight; move heavy logic into `plugins/src/`.

---

## 📄 License

MIT © \[Justin Yang]
