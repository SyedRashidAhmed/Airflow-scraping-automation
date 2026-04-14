FROM apache/airflow:3.0.2
USER root

# Install system dependencies including build tools for pandas
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libnss3 \
    libxss1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    gcc \
    g++ \
    python3-dev \
    build-essential \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV CHROME_BIN=/usr/bin/google-chrome

USER airflow

# Upgrade pip first
RUN pip install --upgrade pip

# Install Python packages in smaller chunks to avoid build issues
RUN pip install --no-cache-dir \
    selenium==4.33.0 \
    beautifulsoup4==4.13.4 \
    requests==2.32.4 \
    yt-dlp==2025.6.9 \
    webdriver-manager==4.0.2

# Install pandas separately with pre-built wheel
RUN pip install --no-cache-dir --only-binary=all pandas==2.0.3

# Install remaining packages, including the new ones
RUN pip install --no-cache-dir \
    pendulum \
    pymongo==4.8.0 \
    python-magic==0.4.27 \
    rapidfuzz==3.9.1 \
    certifi==2024.7.4

# Create necessary directories
RUN mkdir -p /opt/airflow/dags

# Copy Python modules to airflow root for imports
COPY scripts/scraping.py /opt/airflow/
COPY scripts/mongodb_storage.py /opt/airflow/
COPY scripts/urls.txt /opt/airflow/

# Copy DAG files from dags directory
COPY dags/web_scraping_dag.py /opt/airflow/dags/

WORKDIR /opt/airflow