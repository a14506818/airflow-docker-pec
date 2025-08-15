FROM apache/airflow:2.9.0-python3.9

# 🧑‍🔧 切換為 root 安裝系統依賴
USER root

# 🧹 一次性合併安裝所有 apt 套件，並清除快取
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget unzip curl gnupg \
        fonts-liberation libu2f-udev libvulkan1 \
        xvfb libxml2 libssl-dev unixodbc-dev \
        gcc g++ make gosu \
        ca-certificates \
        && rm -rf /var/lib/apt/lists/*

# 🧑‍💻 安裝 Google Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# 🧩 安裝 MS ODBC Driver 17
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    rm -rf /var/lib/apt/lists/*

# 📦 複製 SAP NWRFC SDK，設定環境變數
COPY plugins/nwrfcsdk /opt/nwrfcsdk
ENV SAPNWRFC_HOME=/opt/nwrfcsdk
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/nwrfcsdk/lib

# 👤 切回 airflow user，減少權限風險
USER airflow

# 📦 安裝 Python 套件（合併 pip 安裝，避免中間層疊加）
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
        selenium \
        webdriver-manager \
        pyodbc \
        python-dotenv \
        pyrfc==3.3.1 \
        pandas \
        openpyxl \
        requests
