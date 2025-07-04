# Apache Airflow on Docker 🪂

This repository contains a pre-configured Docker-based setup of [Apache Airflow](https://airflow.apache.org/) using `docker-compose`. It is suitable for development, testing, and deployment of DAGs.

## 🏗️ Stack Overview

- **Apache Airflow** `2.9.x`
- **CeleryExecutor**
- **PostgreSQL** (metadata DB)
- **Redis** (Celery message broker)
- **Docker Compose**
- **Python** `3.9`

## 📁 Folder Structure

```
.
├── dags/                  # Your DAG files
├── logs/                  # Airflow logs (auto-generated)
├── plugins/               # Custom plugins (optional)
├── config/                # Custom config files (optional)
├── .env                   # Environment variables (e.g., AIRFLOW_UID)
├── docker-compose.yaml    # Docker Compose setup
└── README.md
```

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
```

### 2. Set your UID (Linux/macOS)

This ensures proper file permissions between host and containers:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

For Windows WSL2, use:
```bash
echo AIRFLOW_UID=50000 > .env
```

### 3. Initialize Airflow

```bash
docker compose up airflow-init
```

### 4. Start the Airflow services

```bash
docker compose up -d
```

### 5. Access the Airflow UI

- URL: [http://localhost:8080](http://localhost:8080)
- Default login:
  - **Username:** `airflow`
  - **Password:** `airflow`

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

MIT © [Your Name]
