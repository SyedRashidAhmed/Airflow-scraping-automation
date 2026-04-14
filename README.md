# Airflow-scraping-automation

# 📘 (Airflow + Docker + Web Scraping)


# 🕸️ Dockerized Web Scraping & PDF Extraction Pipeline with Airflow

This project is a robust, fully Dockerized web scraping system built with Python and orchestrated via Apache Airflow. It supports dynamic URL configuration, deep crawling, PDF file extraction, and logs all operations. It stores scraped content and files into Docker-managed volumes for persistence and portability.

---

## 📦 Features

- ✅ Recursive web crawling with configurable depth  
- ✅ PDF file validation, download, and text extraction  
- ✅ Dockerized environment with persistent volumes  
- ✅ Apache Airflow DAG for automated and repeatable scraping  
- ✅ URL input via a CSV-style `urls.txt` file  
- ✅ Logging of all scraping attempts and outcomes  

---

## 🛠️ Technologies Used

- Python (Requests, BeautifulSoup, Selenium)
- PDFMiner (for text extraction from PDFs)
- Docker & Docker Compose
- Apache Airflow (with PythonOperator)
- Bash, Linux utilities
- Docker Volumes for persistent storage

---

## 📁 Folder Structure

```

project_root/
│
├── Dockerfile
├── docker-compose.yaml
├── scraping.py                 # Core scraping logic
├── auto_scraping.py           # CLI entrypoint for bulk scraping
├── web_scraping_dag.py        # Airflow DAG definition
├── urls.txt                   # URL input file (format: url,depth,folder)
└── scraped_data              # Docker-managed volume mount for output

```

---

## 📋 Input File Format

The `urls.txt` should be placed in the container path `/opt/airflow/urls.txt`. Each line should follow this format:

```

<url>,<crawl_depth>,<folder_name>

```

Example:

```
https://example.com,2,example_output
https://example.com,2,example_output
https://docs.site.com,1,docs_site
https://docs.site.com,1,docs_site

````

---

## 🚀 How to Run

### 1. Build & Start Services

```bash
docker-compose up --build
````

This will start Airflow (scheduler + webserver), a scraping container, and mount volumes for data storage.

### 2. Access Airflow

Visit: [http://localhost:8080](http://localhost:8080)
Login with the default credentials (`airflow` / `airflow` if not changed)

Activate the DAG: `automated_web_scraper`

---

## 🔁 DAG Overview (`web_scraping_dag.py`)

* Reads `urls.txt` dynamically at DAG runtime
* Creates one task per URL using `PythonOperator`
* Scraping retries: 3 attempts with 15s delay
* Logs are saved per folder in `scrape_log.txt`

---

## 📂 Output

Scraped pages and PDF files are saved inside:

```
/opt/airflow/scraped_data/<folder_name>
```

Each folder contains:

* Extracted HTML/text content
* PDF files
* Log file: `scrape_log.txt`

---

## 🧪 Example

```
URLS_FILE:
https://example.org,2,example_folder

Inside docker:
/opt/airflow/scraped_data/example_folder/
  ├── page1.html
  ├── policy.pdf
  └── scrape_log.txt
```

---

## 🧹 Cleanup

To remove containers:

```bash
docker-compose down
```

To remove volumes/data:

```bash
docker volume rm <volume_name>
```

---

## 📌 Notes

* User agent spoofing is enabled to avoid basic bot blocks.
* The system respects depth limits but does not enforce `robots.txt` unless toggled.
* Selenium is used as a fallback for dynamic content (headless mode).

---

## 🧑‍💻 Author

**Syed Rashid Ahmed**
Data Engineer | Python Developer
Contact: Syed Rashid Ahmed | SyedRashidAhmed | syedrashid123ahmed@gmail.com
