import datetime
import base64
import multiprocessing
import time
from urllib.parse import urlsplit, urlparse
from urllib.robotparser import RobotFileParser
import hashlib
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import xml.etree.ElementTree as ET
import socket
from urllib.parse import urljoin
from functools import lru_cache
import requests

options = Options()
options.add_argument("--headless=new")


class Crawler:
    def __init__(self):
        self.user_agent = "fri-wier-skupina_n"
        self.hostname = socket.gethostname()
        self.ip_address = socket.gethostbyname(self.hostname)
        self.api_base_url = "http://localhost:5000"

        # Use multiprocessing Manager for process-safe shared objects
        manager = multiprocessing.Manager()
        self.stop_event = manager.Event()
        # Replace thread-specific locks with manager dictionaries
        self.site_cache = dict()
        self.page_hash_cache = dict()

        seed_url = "https://slo-tech.com/"

        # Configure session with better connection pooling
        self.session = requests.Session()

        # Configure connection pooling with more connections
        adapter = HTTPAdapter(
            pool_connections=10,  # Reduced per-process to avoid memory issues
            pool_maxsize=25,  # More realistic per process
            max_retries=Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "POST", "PUT"]
            )
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Connection": "keep-alive"
        })

        # Initial request
        self._post_api("/page/frontierlinks", json={
            "from_page_id": None,
            "links": [{"url": seed_url, "relevance": 3}]
        })

        self.robot_parsers = {}
        self.crawl_delays = {}
        self.max_pages = 25000
        self.current_iteration = 0


    # API helper methods for better caching and connection handling
    def _get_api(self, endpoint, params=None):
        """Make a GET request to the API with built-in retry and caching."""
        url = f"{self.api_base_url}{endpoint}"
        return self.session.get(url, params=params)

    def _post_api(self, endpoint, json=None):
        """Make a POST request to the API with built-in retry."""
        url = f"{self.api_base_url}{endpoint}"
        return self.session.post(url, json=json)

    def _put_api(self, endpoint, json=None):
        """Make a PUT request to the API with built-in retry."""
        url = f"{self.api_base_url}{endpoint}"
        return self.session.put(url, json=json)

    @lru_cache(maxsize=1000)
    def get_domain(self, url):
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        return domain

    @lru_cache(maxsize=1000)
    def is_allowed(self, url):
        domain = self.get_domain(url)
        base_url = f"https://{domain}"
        path = urlsplit(url).path

        if domain not in self.robot_parsers:
            robots_url = f"{base_url}/robots.txt"
            rp = RobotFileParser()
            try:
                response = requests.get(robots_url, timeout=5)
                response.raise_for_status()
                rp.parse(response.text.splitlines())
                self.robot_parsers[domain] = rp

                # Save crawl-delay
                delay = rp.crawl_delay(self.user_agent)
                self.crawl_delays[domain] = delay if delay is not None else None
            except Exception as e:
                print(f"[ERROR] Can't fetch robots.txt for {domain}: {e}")
                self.robot_parsers[domain] = None

        rp = self.robot_parsers.get(domain)
        if rp is None:
            return True

        allowed = rp.can_fetch(self.user_agent, path)
        return allowed

    def parse_sitemap(self, sitemap_url):
        try:
            response = self.session.get(sitemap_url, timeout=5)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            return [url.text.strip() for url in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")]
        except Exception as e:
            print(f"[ERROR] Failed to parse sitemap {sitemap_url}: {e}")
            return []

    def get_robots_txt(self, domain):
        robots_url = f"https://{domain}/robots.txt"
        try:
            return self.session.get(robots_url, timeout=5).text
        except Exception as e:
            print(f"[ERROR] Failed to fetch robots.txt for {domain}: {e}")
            return None

    def extract_and_enqueue_sitemap_links(self, rp, domain, page_id):
        sitemaps = rp.site_maps()
        if not sitemaps:
            return ""

        sitemap_content = ""
        for sitemap_url in sitemaps:
            urls = self.parse_sitemap(sitemap_url)
            for url in urls:
                sitemap_content += url + " "
                if self.is_allowed(url):
                    normalized_url = list(self.normalize_url(domain, url))[0]
                    try:
                        self._post_api("/page/frontierlinks", json={
                            "from_page_id": page_id,
                            "links": [{"url": normalized_url, "relevance": 3}],
                        })
                    except Exception as e:
                        print(f"[ERROR] Failed to enqueue links from sitemap: {e}")
        return sitemap_content

    def get_or_create_site(self, domain, page_id):
        # First check shared cache - no lock needed with manager.dict()
        if domain in self.site_cache:
            return self.site_cache[domain]

        # Check if site already exists
        try:
            response = self._get_api("/site/exists", params={"domain": domain})
            response.raise_for_status()
            data = response.json()
            if data.get("exists"):
                # Add to cache - thread-safe with manager.dict()
                self.site_cache[domain] = data["site_id"]
                return data["site_id"]
        except Exception as e:
            print(f"[ERROR] Site existence check failed for {domain}: {e}")
            return None

        # Fetch robots.txt
        robots_content = self.get_robots_txt(domain)
        sitemap_content = None

        # If robots.txt is available, parse it and process sitemaps
        if robots_content:
            rp = RobotFileParser()
            rp.parse(robots_content.splitlines())
            sitemap_content = self.extract_and_enqueue_sitemap_links(rp, domain, page_id)

        # Create the site in the DB
        try:
            response = self._post_api("/site", json={
                "domain": domain,
                "robots_content": robots_content,
                "sitemap_content": sitemap_content
            })
            response.raise_for_status()
            site_id = response.json()["id"]

            # Add to shared cache - thread-safe
            self.site_cache[domain] = site_id

            return site_id
        except Exception as e:
            print(f"[ERROR] Failed to create site {domain}: {e}")
            return None

    @lru_cache(maxsize=100)
    def select_content_type(self, content_type):
        if "text/html" in content_type:
            return "HTML"
        elif "application/pdf" in content_type:
            return "BINARY"
        elif "image/" in content_type:
            return "IMAGE"
        elif "video/" in content_type:
            return "VIDEO"
        else:
            return "UNKNOWN"

    def js_required(self, html):
        if "data-placeholder" in html or "please enable javascript" in html.lower():
            return True
        return False

    def fetch(self, url):
        try:
            response = self.session.get(url, allow_redirects=True)
            page_source = response.text
            status_code = response.status_code
            content_type = response.headers.get("Content-Type", "")
            content_type = self.select_content_type(content_type)

            if url.endswith(".pdf") or "application/pdf" in content_type:
                content_type = "BINARY"
            elif "image/" in content_type:
                content_type = "BINARY"

            print(url, "Content-Type: ", content_type)

            if self.js_required(page_source):
                driver = webdriver.Firefox(options=options)
                driver.get(url)
                page_source = driver.page_source

                cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
                response = self.session.get(url, cookies=cookies)
                status_code = response.status_code
                content_type = response.headers.get("Content-Type", "")
                content_type = self.select_content_type(content_type)
                driver.close()

            return page_source, status_code, content_type

        except Exception as e:
            print(f"Request failed: {e}")
            return None, 500, None

    def determine_page_type(self, url):
        url = url.lower()

        if url.endswith((".html", ".htm", "robots.txt")):
            return "HTML"
        elif url.endswith(".pdf"):
            return "BINARY"
        elif url.endswith(".jpg") or url.endswith(".jpeg") or url.endswith(".png"):
            return "BINARY" 
        elif url.endswith(".doc"):
            return "DOC"
        elif url.endswith(".docx"):
            return "DOCX"
        elif url.endswith(".ppt"):
            return "PPT"
        elif url.endswith(".pptx"):
            return "PPTX"
        else:
            return "Unknown"

    def normalize_url(self, base_url, links):
        if isinstance(links, str):
            links = [links]

        normalized_links = set()
        for link in links:
            if link == "javascript:void(0)":
                continue

            parsed_link = urlsplit(link)

            if not parsed_link.scheme:
                link = urljoin(base_url, link)

            link = link.strip()
            link = link.lower()

            link = link.split('#')[0]
            link = link.split('?')[0]

            if link.endswith('/'):
                link = link[:-1]

            if link.endswith(("index.html", "index.htm", "default.asp", "default.aspx")):
                link = link.rsplit("/", 1)[0]

            normalized_links.add(link)
        return normalized_links

    def extract_links(self, url, soup):
        links = set()
        for a_tag in soup.find_all("a"):
            href = a_tag.get("href")
            if href:
                links.add(href)

        for tag in soup.find_all():
            if tag.has_attr("onclick"):
                links.add(tag["onclick"])

        links = self.normalize_url(url, links)
        return links

    def extract_images(self, url, soup):
        images = set()
        for img_tag in soup.find_all("img"):
            src = img_tag.get("src")
            if src:
                images.add(src)

        images = self.normalize_url(url, images)
        return images

    def check_relevance(self, links):
        relevant_links = []
        for link in links:
            if self.is_allowed(link):
                relevance = 0
                link_domain = self.get_domain(link)
                if link_domain == "slo-tech.com":
                    relevance += 1
                if any(keyword in link for keyword in ("novice", "forum", "clanki")):
                    relevance += 1

                relevant_links.append((link, relevance))
        return relevant_links

    def hash_html(self, html_content):
        return hashlib.sha256(html_content.encode("utf-8")).hexdigest()

    def check_duplicate(self, page_hash):
        # First check shared cache - no lock needed
        if page_hash in self.page_hash_cache:
            return self.page_hash_cache[page_hash]

        try:
            response = self._get_api("/page/exists", params={"content_hash": page_hash})
            response.raise_for_status()
            result = response.json()

            # Cache the result in shared dictionary
            self.page_hash_cache[page_hash] = result

            return result
        except Exception as e:
            print(f"Error checking for duplicate: {e}")
            return {"exists": False}

    def handle_duplicate_page(self, page_id, original_page_id, duplicate_url, status_code):
        try:
            duplicate_page_data = {
                "site_id": original_page_id,
                "page_type_code": "DUPLICATE",
                "html_content": None,
                "content_hash": None,
                "http_status_code": status_code,
                "accessed_ip": self.ip_address,
                "relevance": 0
            }

            response = self._put_api("/page/" + str(page_id), json=duplicate_page_data)
            response.raise_for_status()

            link_data = {
                "from_page": original_page_id,
                "to_page": page_id
            }

            link_response = self._post_api("/link", json=link_data)
            link_response.raise_for_status()

            print(f"Duplicate page {duplicate_url} linked to original page ID {original_page_id}")
        except Exception as e:
            print(f"Error handling duplicate page {duplicate_url}: {e}")

    def worker(self, stop_event):
        while not stop_event.is_set():
            start = time.time()

            if self.current_iteration % 10 == 0:
                html_response = self._get_api("/page/html-count").json()
                html_count = html_response["html_page_count"]

                if html_count > self.max_pages:
                    print(f"[{multiprocessing.current_process().name}] {self.max_pages} pages have been retrieved.")
                    stop_event.set()
                    break

            try:
                frontier_response = self._get_api("/frontier").json()
                page_id = frontier_response["id"]
                url = frontier_response["url"]
            except:
                print("Frontier is empty.")
                break

            print(f"Crawling URL: {url}")
            domain = self.get_domain(url)
            site_id = self.get_or_create_site(domain, page_id)
            print(f'Site ID: {site_id}')

            crawl_delay = self.crawl_delays.get(domain)
            delay = self._post_api("/site/delay", json={"site_url": domain, "ip": self.ip_address, "robots_delay": crawl_delay}).json()
            print(f"Delay: {delay['delay']}")
            time.sleep(delay["delay"])

            print(f"Fetching: {url}")
            html, status_code, content_type = self.fetch(url)

            print("TYPE ", content_type)
            if content_type != "HTML":
                try:
                    page_data = {
                        "page_id": page_id,
                        "data_type_code": "BINARY", 
                        "data": None 
                    }
                    print(f"Inserting {content_type} metadata into page_data table for {url}")
                    self._post_api("/pagedata", json=page_data)

                    self._put_api("/page/" + str(page_id), json={
                        "page_type_code": "BINARY",  
                        "html_content": None,
                        "http_status_code": status_code,
                        "accessed_ip": self.ip_address,
                        "site_id": site_id,
                        "content_hash": None
                    })
                    print(f"Processed and marked {content_type} as BINARY for {url}")

                    """
                    if content_type == "application/pdf":
                        pdf_response = requests.get(url)
                        pdf_content = pdf_response.content 
                        if not pdf_content:
                            print(f"Warning: No content found for PDF at {url}")
                            continue  

                        encoded_pdf_data = base64.b64encode(pdf_content).decode('utf-8') 

                        page_data = {
                            "page_id": page_id,
                            "data_type_code": "BINARY", 
                            "data": encoded_pdf_data 
                        }
                        self._post_api("/pagedata", json=page_data)

                        self._put_api("/page/" + str(page_id), json={
                            "page_type_code": "BINARY", 
                            "html_content": None,
                            "http_status_code": status_code,
                            "accessed_ip": self.ip_address,
                            "site_id": site_id,
                            "content_hash": None
                        })

                    else:
                        page_data = {
                            "page_id": page_id,
                            "data_type_code": "BINARY", 
                            "data": None
                        }
                        self._post_api("/pagedata", json=page_data)

                        self._put_api("/page/" + str(page_id), json={
                            "page_type_code": "BINARY",  
                            "html_content": None,
                            "http_status_code": status_code,
                            "accessed_ip": self.ip_address,
                            "site_id": site_id,
                            "content_hash": None
                        })
                    """

                except Exception as e:
                    print(f"Error while handling non-HTML content: {e}")
                    continue
                self.current_iteration += 1
                continue

            soup = BeautifulSoup(html, "html.parser")

            for script in soup.find_all("script"):
                script.decompose()

            for style in soup.find_all("style"):
                style.decompose()

            for meta in soup.find_all("meta"):
                meta.decompose()

            normalized_html = soup.prettify()
            page_hash = self.hash_html(normalized_html)

            duplicate = self.check_duplicate(page_hash)

            print(f'Page duplicate: {duplicate}')

            if duplicate.get("exists", False):
                self.handle_duplicate_page(page_id, duplicate["page_id"], url, status_code)
            else:
                try:
                    self._put_api("/page/" + str(page_id), json={
                        "page_type_code": content_type,
                        "html_content": normalized_html,
                        "http_status_code": status_code,
                        "accessed_ip": self.ip_address,
                        "site_id": site_id,
                        "content_hash": page_hash
                    })
                except Exception as e:
                    print(f"Error while updating page: {e}")
                    continue

                links = self.extract_links(url, soup)
                images = self.extract_images(url, soup)

                print(f'Found {len(links)} links and {len(images)} images.')

                relevant_links = self.check_relevance(links)

                try:
                    self._post_api("/page/frontierlinks", json={
                        "from_page_id": page_id,
                        "links": [{"url": url, "relevance": relevance} for url, relevance in relevant_links]
                    })
                    print(f"Updated frontier links for {url}")
                except Exception as e:
                    print(f"Error while updating frontier: {e}")
                    continue

            """
            for img_url in images:
                try:
                    img_content = requests.get(img_url).content
                    encoded_image_data = base64.b64encode(img_content).decode('utf-8')

                    image_data = {
                        "page_id": page_id,
                        "filename": img_url.split("/")[-1],  
                        "content_type": "image", 
                        "data": encoded_image_data, 
                        "accessed_time": datetime.datetime.utcnow().isoformat()
                    }

                    self._post_api("/image", json=image_data)
                    print(f"Inserted image: {img_url}")
                except Exception as e:
                    print(f"Error inserting image: {img_url} - {e}")

                
            """

            self.current_iteration += 1
            end = time.time()
            print(f"Time elapsed: {end - start:.2f}s")

    def run(self, num_workers=2):
        args = (self.stop_event,)
        processes = [multiprocessing.Process(target=self.worker, args=args) for _ in range(num_workers)]

        for i, p in enumerate(processes):
            time.sleep(i*5)
            p.start()
        for p in processes:
            p.join()

        print("Crawling complete.")

if __name__ == '__main__':
    crawler = Crawler()
    crawler.run(num_workers=3)
