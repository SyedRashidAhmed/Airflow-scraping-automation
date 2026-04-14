FROM apache/airflow:3.0.2
USER root

# Install dependencies and Google Chrome
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
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV CHROME_BIN=/usr/bin/google-chrome

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    selenium==4.33.0 \
    beautifulsoup4==4.13.4 \
    requests==2.32.4 \
    yt-dlp==2025.6.9 \
    webdriver-manager==4.0.2 \
    pendulum

# Copy your scraping logic and config
COPY auto_scraping.py /opt/airflow/
COPY scraping.py /opt/airflow/
COPY urls.txt /opt/airflow/

WORKDIR /opt/airflow