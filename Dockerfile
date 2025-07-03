FROM apache/airflow:2.9.0-python3.9

# 用 root 做初始設定
USER root

# 安裝基本工具與依賴套件
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    gnupg \
    fonts-liberation \
    libu2f-udev \
    libvulkan1 \
    xvfb \
    libxml2 \
    libssl-dev \
    unixodbc-dev \
    gcc \
    g++ \
    make \
    gosu \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# 安裝 Chrome & Chrome Driver
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list
RUN apt-get update && apt-get -y install google-chrome-stable

# 安裝 Microsoft ODBC Driver for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 複製 SAP NWRFC SDK
COPY plugins/nwrfcsdk /opt/nwrfcsdk
# 設定環境變數
ENV SAPNWRFC_HOME=/opt/nwrfcsdk
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/nwrfcsdk/lib

USER airflow    
RUN pip install --upgrade pip
RUN pip install --no-cache-dir selenium
RUN pip install --no-cache-dir webdriver-manager
RUN pip install --no-cache-dir pyodbc
RUN pip install --no-cache-dir python-dotenv

RUN pip install --no-cache-dir pyrfc==3.3.1
RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir openpyxl


