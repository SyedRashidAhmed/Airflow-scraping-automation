import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException, StaleElementReferenceException
import argparse
import re
import sys
import time
import json
import os
from urllib.parse import urlparse, urljoin, urlunparse
from webdriver_manager.chrome import ChromeDriverManager
import hashlib
from urllib.robotparser import RobotFileParser
from yt_dlp import YoutubeDL
import mimetypes
def clean_text(text):
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n+', '\n', text)
    return text.strip()
def normalize_url(url):
    parsed = urlparse(url)
    normalized = parsed._replace(query="", fragment="")
    return urlunparse(normalized)
def hash_content(html):
    return hashlib.md5(html.encode('utf-8')).hexdigest()
import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
def setup_driver(headless=True, user_agent=None):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-extensions")
    if not user_agent:
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        )
    chrome_options.add_argument(f"--user-agent={user_agent}")
    prefs = {
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "profile.default_content_setting_values.ads": 2,
        "profile.managed_default_content_settings.plugins": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    try:
        os.environ["WDM_LOG_LEVEL"] = "0"
        os.environ["WDM_LOCAL"] = "0"
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    except Exception as e:
        print(f"❌ Error setting up ChromeDriver: {type(e).__name__}: {e}")
        sys.exit(1)
def is_pdf_url(url, file_types=("pdf", "csv", "xlsx", "xls", "json", "docx", "doc", "txt", "mp4", "webm", "ogg", "avi", "mov", "mkv")):
    """Check if the URL points to a downloadable file."""
    if not url:
        return False
    parsed = urlparse(url)
    path = parsed.path.lower()
    if any(path.endswith(f".{ext}") for ext in file_types):
        return True
    try:
        head = requests.head(url, allow_redirects=True, timeout=10)
        content_type = head.headers.get('Content-Type', '').lower()
        return any(vtype in content_type for vtype in ['video/', 'application/', 'text/csv', 'text/plain'])
    except requests.RequestException:
        return False
def download_pdf(url, file_type_dirs, file_counter,
                 file_types=("pdf", "csv", "xlsx", "xls", "json", "docx", "doc", "txt", "mp4", "webm", "ogg", "avi", "mov", "mkv", "mp3", "wav", "m4a"),
                 robot_parser=None, user_agent=None, downloaded_files=None):
    if robot_parser and not robot_parser.can_fetch(user_agent, url):
        print(f"❌ Blocked by robots.txt: {url}")
        return None
    headers = {'User-Agent': user_agent} if user_agent else {}
    if any(domain in url for domain in ["youtube.com", "youtu.be", "vimeo.com", "twitter.com", "x.com", "instagram.com", "facebook.com"]):
        try:
            save_dir = file_type_dirs.get("audio", file_type_dirs["others"])
            os.makedirs(save_dir, exist_ok=True)
            ydl_opts = {
                'format': 'bestaudio/best',
                'extractaudio': True,
                'audioformat': 'mp3',
                'outtmpl': os.path.join(save_dir, f"audio_{file_counter}_%(title).50s.%(ext)s"),
                'quiet': False,
                'noplaylist': True,
            }
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                downloaded_file = ydl.prepare_filename(info)
                print(f"✅ Audio downloaded using yt-dlp: {downloaded_file}")
                return downloaded_file
        except Exception as yt_error:
            print(f"❌ yt-dlp failed for {url} ({yt_error})")
            return None
    try:
        response = requests.get(url, stream=True, headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"❌ Failed to fetch: {url} (status {response.status_code})")
            return None
        content_type = response.headers.get("Content-Type", "").lower()
        ext_map = {
            "application/pdf": "pdf", "text/csv": "csv", "application/vnd.ms-excel": "xls",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx", "application/json": "json",
            "application/msword": "doc", "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
            "text/plain": "txt", "audio/mpeg": "audio", "audio/mp3": "audio", "audio/wav": "audio",
            "video/mp4": "audio", "video/webm": "audio", "video/ogg": "audio", "video/x-msvideo": "audio",
            "video/quicktime": "audio", "video/x-matroska": "audio"
        }
        ext = ext_map.get(content_type)
        if not ext:
            path = urlparse(url).path.lower()
            ext = next((ftype for ftype in file_types if path.endswith(f".{ftype}")), "others")
        if content_type.startswith("video/") or content_type.startswith("audio/") or ext in ["mp3", "wav", "m4a", "mp4", "webm", "ogg", "avi", "mov", "mkv"]:
            ext = "audio"
        save_dir = file_type_dirs.get(ext, file_type_dirs["others"])
        os.makedirs(save_dir, exist_ok=True)
        filename = os.path.basename(urlparse(url).path)
        if not filename or '.' not in filename:
            filename = f"file_{file_counter}.mp3" if ext == "audio" else f"file_{file_counter}.{ext}"
        output_path = os.path.join(save_dir, filename)
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Downloaded: {url} → {output_path}")
        if downloaded_files is not None:
            downloaded_files.add(url)
        return output_path
    except Exception as e:
        print(f"❌ Error downloading from {url}: {e}")
        return None
def extract_element_data(element, include_attrs=False):
    if element.name is None:
        text = clean_text(element.string or '')
        return {"type": "text", "content": text} if text else None
    element_info = {
        "type": "element",
        "tag": element.name,
        "children": []
    }
    if element.get('id'):
        element_info["id"] = element.get('id')
    if element.get('class'):
        element_info["classes"] = ' '.join(element.get('class'))
    if include_attrs:
        attrs = {
            k: ' '.join(v) if isinstance(v, list) else v
            for k, v in element.attrs.items() if k not in ['id', 'class']
        }
        if attrs:
            element_info["attributes"] = attrs
    direct_text = clean_text(''.join(element.strings))
    if direct_text:
        element_info["text"] = direct_text
    for child in element.children:
        child_data = extract_element_data(child, include_attrs)
        if child_data:
            element_info["children"].append(child_data)
    return element_info
def extract_structured_content(soup, url, format='text', include_attrs=False):
    title = soup.title.string if soup.title else "No title found"
    for tag in soup(['script', 'style', 'meta', 'link']):
        tag.decompose()
    if format in ['json', 'detailed']:
        body = soup.body or soup
        structure = extract_element_data(body, include_attrs)
        result = {
            "url": url,
            "domain": urlparse(url).netloc,
            "title": title,
            "structure": structure
        }
        return json.dumps(result, indent=2) if format == 'json' else "\n".join(format_detailed_text(result))
    output, seen_content = [f"URL: {url}", f"Title: {title}", ""], set()
    for el in soup.find_all(['div', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'article', 'section', 'main', 'aside', 'header', 'footer', 'nav', 'ul', 'ol', 'li', 'table']):
        text = clean_text(el.get_text())
        if text and text not in seen_content:
            seen_content.add(text)
            output.append("")
            output.append(text)
    final_output, prev_blank = [], False
    for line in output:
        if line.strip() == "":
            if not prev_blank:
                final_output.append("")
                prev_blank = True
        else:
            final_output.append(line)
            prev_blank = False
    return "\n".join(final_output)
def format_detailed_text(data, indent=0):
    lines = []
    prefix = " " * indent
    if indent == 0:
        lines.append(f"URL: {data['url']}")
        lines.append(f"Domain: {data['domain']}")
        lines.append(f"Title: {data['title']}")
        lines.append("")
        data = data['structure']
    tag_info = f"{prefix}<{data['tag']}"
    if 'id' in data:
        tag_info += f" id=\"{data['id']}\""
    if 'classes' in data:
        tag_info += f" class=\"{data['classes']}\""
    tag_info += ">"
    lines.append(tag_info)
    if 'text' in data and data['text'].strip():
        lines.append(f"{prefix}  TEXT: {data['text']}")
    for child in data.get('children', []):
        lines.extend(format_detailed_text(child, indent + 2))
    return lines
def find_clickable_elements(driver):
    """Find clickable elements by their selectors directly (not by storing elements)"""
    selectors = [
        {"type": "Button", "selector": By.TAG_NAME, "value": "button"},
        {"type": "Link", "selector": By.TAG_NAME, "value": "a"},
        {"type": "Input", "selector": By.CSS_SELECTOR, "value": "input[type='submit'], input[type='button']"},
        {"type": "Clickable", "selector": By.CSS_SELECTOR, "value": "[onclick], [role='button'], [class*='btn'], [class*='button']"}
    ]
    return selectors
def collect_pdf_links_by_selector(driver, base_url, file_types=("pdf", "csv", "xlsx", "xls", "json", "docx", "doc")):
    file_links = []
    try:
        links = driver.find_elements(By.TAG_NAME, "a")
        for link in links:
            try:
                href = link.get_attribute("href")
                if href and is_pdf_url(href, file_types):
                    text = link.text or os.path.basename(urlparse(href).path) or "Download"
                    full_url = urljoin(base_url, href)
                    file_links.append((full_url, text))
            except StaleElementReferenceException:
                continue
    except Exception as e:
        print(f"Error collecting file links: {e}")
    return file_links
def safe_click_by_selector(driver, selector_type, selector_value, wait_time, tried_selectors):
    """Click elements safely by re-finding them each time"""
    current_url = driver.current_url
    clicked_something = False
    try:
        elements = driver.find_elements(selector_type, selector_value)
        valid_elements = []
        for i, element in enumerate(elements):
            try:
                element_id = None
                if element.get_attribute("id"):
                    element_id = f"id:{element.get_attribute('id')}"
                elif element.get_attribute("name"):
                    element_id = f"name:{element.get_attribute('name')}"
                elif element.get_attribute("class"):
                    element_id = f"class:{element.get_attribute('class')}_{i}"
                else:
                    text = element.text if element.text else ""
                    element_id = f"text:{text[:20]}_{i}"
                if element_id in tried_selectors:
                    continue
                valid_elements.append((element, element_id))
            except StaleElementReferenceException:
                continue
            except Exception as e:
                continue
        for element, element_id in valid_elements:
            try:
                tried_selectors.add(element_id)
                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(0.5)
                element.click()
                time.sleep(wait_time)
                new_url = driver.current_url
                if new_url != current_url:
                    print(f"Successfully clicked {element_id}, URL changed to: {new_url}")
                    clicked_something = True
                    return True, new_url
                print(f"Clicked {element_id}, but URL remained the same")
            except StaleElementReferenceException:
                print(f"Element became stale during click: {element_id}")
                continue
            except Exception as e:
                print(f"Error clicking {element_id}: {str(e)[:100]}")
                continue
    except Exception as e:
        print(f"Error finding or interacting with elements: {e}")
    return clicked_something, driver.current_url
def setup_robots_parser(base_url, user_agent):
    """Set up robots.txt parser for a given base URL"""
    try:
        parsed_url = urlparse(base_url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        rp = RobotFileParser()
        rp.set_url(robots_url)
        print(f"Reading robots.txt from: {robots_url}")
        rp.read()
        if rp.mtime() == 0:
            print("No robots.txt found or couldn't be parsed, proceeding with caution")
        else:
            print("Successfully loaded robots.txt")
        return rp
    except Exception as e:
        print(f"Error setting up robots parser: {e}")
        rp = RobotFileParser()
        return rp
def extract_and_navigate(url, output_dir, max_depth=2, interactive=False, headless=True, format='text',
                         include_attrs=False, wait_time=1, infinite=False, respect_robots=True, user_agent=None):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_type_dirs = {
        "pdf": os.path.join(output_dir, "downloads", "pdf"),
        "csv": os.path.join(output_dir, "downloads", "csv"),
        "xlsx": os.path.join(output_dir, "downloads", "excel"),
        "xls": os.path.join(output_dir, "downloads", "excel"),
        "json": os.path.join(output_dir, "downloads", "json"),
        "docx": os.path.join(output_dir, "downloads", "word"),
        "doc": os.path.join(output_dir, "downloads", "word"),
        "txt": os.path.join(output_dir, "downloads", "text"),
        "audio": os.path.join(output_dir, "downloads", "audio"),
        "others": os.path.join(output_dir, "downloads", "others")
    }
    for dir_path in file_type_dirs.values():
        os.makedirs(dir_path, exist_ok=True)
    if not user_agent:
        user_agent = "Mozilla/5.0 (compatible; MyWebCrawler/1.0; +https://example.com/bot)"
    robot_parser = None
    if respect_robots:
        robot_parser = setup_robots_parser(url, user_agent)
    driver = setup_driver(headless, user_agent)
    visited_urls = {}
    content_hashes = set()
    downloaded_files = set()
    normalized_to_url = {}
    state = {'pdf_counter': 1}
    def process_page(current_url, depth=0, tried_selectors=None):
        if tried_selectors is None:
            tried_selectors = set()
        normalized_url = normalize_url(current_url)
        if (not infinite and depth > max_depth) or normalized_url in visited_urls:
            return
        video_domains = (
            'youtube.com', 'youtu.be', 'vimeo.com', 'twitter.com',
            'x.com', 'instagram.com', 'tiktok.com', 'facebook.com'
        )
        domain = urlparse(current_url).netloc.lower()
        is_known_video = any(vd in domain for vd in video_domains)
        if is_known_video:
            if normalized_url not in downloaded_files:
                file_path = download_pdf(
                    current_url,
                    file_type_dirs,
                    state['pdf_counter'],
                    robot_parser=robot_parser,
                    user_agent=user_agent,
                    downloaded_files=downloaded_files
                )
                if file_path:
                    state['pdf_counter'] += 1
            return
        try:
            if is_pdf_url(current_url):
                head = requests.head(current_url, allow_redirects=True, timeout=5)
                content_type = head.headers.get("Content-Type", "").lower()
                if not content_type.startswith("text/html"):
                    if normalized_url not in downloaded_files:
                        file_path = download_pdf(
                            current_url,
                            file_type_dirs,
                            state['pdf_counter'],
                            robot_parser=robot_parser,
                            user_agent=user_agent,
                            downloaded_files=downloaded_files
                        )
                        if file_path:
                            state['pdf_counter'] += 1
                    return
        except requests.RequestException:
            pass
        try:
            print(f"🌐 Navigating to: {current_url}")
            driver.get(current_url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(wait_time)
            html = driver.execute_script("return document.documentElement.outerHTML")
            content_hash = hash_content(html)
            if content_hash in content_hashes:
                print(f"🔁 Duplicate content skipped: {current_url}")
                return
            content_hashes.add(content_hash)
            soup = BeautifulSoup(html, 'html.parser')
            links = collect_pdf_links_by_selector(driver, current_url)
            for file_url, _ in links:
                normalized_file_url = normalize_url(file_url)
                if normalized_file_url not in downloaded_files:
                    if not respect_robots or robot_parser.can_fetch(user_agent, file_url):
                        file_path = download_pdf(
                            file_url,
                            file_type_dirs,
                            state['pdf_counter'],
                            robot_parser=robot_parser,
                            user_agent=user_agent,
                            downloaded_files=downloaded_files
                        )
                        if file_path:
                            state['pdf_counter'] += 1
                    else:
                        print(f"🚫 Skipping due to robots.txt: {file_url}")
            filename_base = re.sub(r'[^\w]', '_', urlparse(normalized_url).path or 'index')
            filename = f"{filename_base or 'index'}_{depth}"
            content = extract_structured_content(soup, current_url, format, include_attrs)
            output_path = os.path.join(output_dir, f"{filename}.{format}")
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            text_only = extract_structured_content(soup, current_url, format='text', include_attrs=False)
            text_download_path = os.path.join(file_type_dirs['txt'], f"{filename}.txt")
            with open(text_download_path, 'w', encoding='utf-8') as f:
                f.write(text_only)
            visited_urls[normalized_url] = output_path
            normalized_to_url[normalized_url] = current_url
            for link in soup.find_all("a", href=True):
                href = link['href']
                joined_url = urljoin(current_url, href)
                norm_link = normalize_url(joined_url)
                if (
                    urlparse(norm_link).hostname and
                    urlparse(norm_link).hostname.endswith(urlparse(url).hostname) and
                    norm_link not in visited_urls and
                    norm_link != normalized_url
                ):
                    if not respect_robots or robot_parser.can_fetch(user_agent, joined_url):
                        process_page(joined_url, depth + 1)
                    else:
                        print(f"🚫 Skipping link (robots.txt): {joined_url}")
        except Exception as e:
            print(f"❌ Error processing {current_url}: {e}")
    process_page(url)
    print(f"Downloaded {len(downloaded_files)} files.")
    return visited_urls, downloaded_files
def main():
    parser = argparse.ArgumentParser(description='Dynamic website content scraper with navigation, PDF extraction, and robots.txt support.')
    parser.add_argument('url', help='Starting URL to scrape')
    parser.add_argument('-o', '--output', default='extracted_content', help='Output directory')
    parser.add_argument('-d', '--depth', type=int, default=2, help='Max navigation depth')
    parser.add_argument('-i', '--interactive', action='store_true', help='Enable interactive mode')
    parser.add_argument('-v', '--visible', action='store_true', help='Make browser visible')
    parser.add_argument('-f', '--format', choices=['text', 'json', 'detailed'], default='text', help='Output format')
    parser.add_argument('-a', '--attributes', action='store_true', help='Include HTML attributes')
    parser.add_argument('-w', '--wait', type=float, default=1, help='Wait time after load (seconds)')
    parser.add_argument('--infinite', action='store_true', help='Scrape until no new unique pages are found')
    parser.add_argument('-r', '--robots', action='store_true', default=False, help='Respect robots.txt rules')
    parser.add_argument('-u', '--user-agent', default=None, help='Set custom user agent')
    try:
        args = parser.parse_args()
        url = args.url if args.url.startswith(('http://', 'https://')) else 'https://' + args.url
        print(f"Extracting from: {url}")
        print(f"Respecting robots.txt: {'Yes' if args.robots else 'No'}")
        visited, pdfs = extract_and_navigate(
            url=url,
            output_dir=args.output,
            max_depth=args.depth,
            interactive=args.interactive,
            headless=not args.visible,
            format=args.format,
            include_attrs=args.attributes,
            wait_time=args.wait,
            infinite=args.infinite,
            respect_robots=args.robots,
            user_agent=args.user_agent
        )
        print(f"\nDone. {len(visited)} pages and {len(pdfs)} files saved in {args.output}/")
        print(json.dumps({
            "status": "success",
            "pages_saved": len(visited),
            "files_downloaded": len(pdfs),
            "output_dir": args.output
        }))
    except Exception as e:
        import traceback
        error_message = f"{type(e).__name__}: {str(e)}"
        print(traceback.format_exc())
if __name__ == "__main__":
    main()