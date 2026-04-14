from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import pendulum
import os
import sys
sys.path.append('/opt/airflow')
from scraping import extract_and_navigate
URLS_FILE_PATH = "/opt/airflow/urls.txt"
SCRAPED_DATA_FOLDER = "/opt/airflow/scraped_data"
local_tz = pendulum.timezone("Asia/Kolkata")
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
}
def scrape_task(url, depth, folder, **kwargs):
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/58.0.3029.110 Safari/537.3"
    os.makedirs(folder, exist_ok=True)
    log_file = os.path.join(folder, "scrape_log.txt")
    def log(msg):
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now()}] {msg}\n")
    attempt, success = 0, False
    while attempt < 3 and not success:
        try:
            log(f"Attempt {attempt+1} - Scraping {url}")
            visited, files = extract_and_navigate(
                url=url,
                output_dir=folder,
                max_depth=depth if depth else 100,
                interactive=False,
                headless=True,
                format='text',
                include_attrs=False,
                wait_time=1,
                infinite=(depth is None),
                respect_robots=False,
                user_agent=user_agent
            )
            log(f"✅ Success - Pages: {len(visited)} | Files: {len(files)}")
            success = True
        except Exception as e:
            log(f"❌ Error: {e}")
            attempt += 1
def read_urls():
    urls = []
    if not os.path.exists(URLS_FILE_PATH):
        raise FileNotFoundError(f"URL file not found: {URLS_FILE_PATH}")
    with open(URLS_FILE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(',')
            if not parts or len(parts) < 2:
                continue
            url = parts[0].strip()
            depth = int(parts[1].strip()) if parts[1].strip().isdigit() else None
            folder = os.path.join(SCRAPED_DATA_FOLDER, parts[2].strip()) if len(parts) >= 3 else os.path.join(SCRAPED_DATA_FOLDER, "output")
            urls.append((url, depth, folder))
    return urls
local_tz = pendulum.timezone("Asia/Kolkata")
with DAG(
    dag_id='automated_web_scraper',
    default_args=default_args,
    schedule=timedelta(days=1),
    start_date=pendulum.now(tz=local_tz).subtract(minutes=5),
    catchup=False,
    tags=['scraper', 'web'],
    description='Scrapes websites using dynamic URLs from a file.'
) as dag:
    for i, (url, depth, folder) in enumerate(read_urls()):
        safe_folder_name = os.path.basename(folder).replace(" ", "_")
        PythonOperator(
            task_id=f"scrape_{i}_{safe_folder_name}",
            python_callable=scrape_task,
            op_kwargs={'url': url, 'depth': depth, 'folder': folder},
        )