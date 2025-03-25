import multiprocessing
import time
from urllib.parse import urlsplit, urlparse
from urllib.robotparser import RobotFileParser
import hashlib
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import xml.etree.ElementTree as ET
import socket
from urllib.parse import urljoin

options = Options()
options.add_argument("--headless=new")


class Crawler:
    def __init__(self):
        self.user_agent = "fri-wier-skupina_n"
        self.hostname = socket.gethostname()
        self.ip_address = socket.gethostbyname(self.hostname)
        self.frontier = multiprocessing.Queue()
        seed_url = "https://slo-tech.com/"
        self.frontier.put(seed_url)

        manager = multiprocessing.Manager()
        self.stop_event = manager.Event()

        # Robots.txt for each domain
        self.robot_parsers = {}

        ## TO DO: read parsed_urls and last access from database?
        self.last_access_times = manager.dict()
        self.last_access_ips = manager.dict()
        self.max_pages = 2

    def get_robot_parser(self, domain):

        if domain not in self.robot_parsers:
            rp = RobotFileParser()
            robots_url = domain + "/robots.txt"
            try:
                rp.set_url(robots_url)
                rp.read()
                self.robot_parsers[domain] = rp
            except Exception as e:
                print(f"Error while reading robots.txt for {domain}: {e}")
                self.robot_parsers[domain] = None

            sitemaps = rp.site_maps()
            if sitemaps:
                for sitemap_url in sitemaps:
                    sitemap_urls = self.parse_sitemap(sitemap_url)
                    for url in sitemap_urls:
                        if self.determine_page_type(url) in ("HTML", "Unknown") and self.is_allowed(url):
                            normalized_urls = self.normalize_url(domain, url)
                            self.frontier.put(list(normalized_urls)[0])
        return self.robot_parsers[domain]

    def get_ip(self, url):
        try:
            domain = urlsplit(url).netloc
            return socket.gethostbyname(domain)
        except Exception as e:
            print(f"Error retrieving IP for {url}: {e}")
            return None

    def respect_crawl_delay(self, domain, url, last_access_times, last_access_ips):
        parser = self.get_robot_parser(domain)
        delay = 5

        if parser:
            crawl_delay = parser.crawl_delay(self.user_agent)
            if crawl_delay is not None:
                delay = crawl_delay

        now = time.time()

        # Last access time by domain
        domain_last = last_access_times.get(domain, 0)
        domain_wait = delay - (now - domain_last)

        # Last access time by IP
        ip = self.get_ip(url)
        ip_last = last_access_ips.get(ip, 0) if ip else 0
        ip_wait = delay - (now - ip_last) if ip else 0

        # Final wait time is the maximum of both
        final_wait = max(domain_wait, ip_wait, 0)

        if final_wait > 0:
            print(f"Waiting {final_wait:.2f}s for {domain} / {ip}")
            time.sleep(final_wait)

        # Update last access time
        last_access_times[domain] = time.time()
        if ip:
            last_access_ips[ip] = time.time()

    def is_allowed(self, url):
        domain = "{0.scheme}://{0.netloc}".format(urlsplit(url))
        parser = self.get_robot_parser(domain)
        if parser:
            if parser.can_fetch(self.user_agent, url) == False:
                print("false")
            return parser.can_fetch(self.user_agent, url)
        return True

    def parse_sitemap(self, sitemap_url):
        try:
            response = requests.get(sitemap_url)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            urls = []

            for url in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                url_text = url.text.strip()
                urls.append(url_text)
            return urls
        except Exception as e:
            print(f"Error while reading sitemap: {sitemap_url}: {e}")
            return []

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

    def fetch(self, url):
        driver = webdriver.Firefox(options=options)
        driver.get(url)
        page_source = driver.page_source

        cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
        response = requests.get(url, cookies=cookies)
        status_code = response.status_code
        content_type = response.headers.get("Content-Type", "")
        content_type = self.select_content_type(content_type)
        driver.close()

        return page_source, status_code, content_type

    def determine_page_type(self, url):
        url = url.lower()

        if url.endswith((".html", ".htm", "robots.txt")):
            return "HTML"
        elif url.endswith(".pdf"):
            return "PDF"
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

    def page_type_html(self, url):
        url = url.lower()

        if url.endswith(("robots.txt")):
            return "HTML"

        try:
            response = requests.head(url, timeout=5, allow_redirects=True)
            content_type = response.headers.get('Content-Type', '')
            if "text/html" in content_type:
                return "HTML"
            else:
                return "Unknown"
        except Exception as e:
            print(f"[HEAD ERROR] {url}: {e}")

    def normalize_url(self, base_url, links):
        if isinstance(links, str):
            links = [links]

        normalized_links = set()
        for link in links:
            # Parse the link
            parsed_link = urlsplit(link)

            # If the link is relative, make it absolute
            if not parsed_link.scheme:
                link = urljoin(base_url, link)

            # Canonicalize the URL
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
        #soup = BeautifulSoup(html, "html.parser")
        links = set()
        for a_tag in soup.find_all("a"):
            href = a_tag.get("href")
            if href:
                links.add(href)

        # Include links from onclick attributes
        for tag in soup.find_all():
            if tag.has_attr("onclick"):
                links.add(tag["onclick"])

        # Correctly extend relative links
        links = self.normalize_url(url, links)
        return links

    def extract_images(self, url, soup):
        #soup = BeautifulSoup(html, "html.parser")
        images = set()
        for img_tag in soup.find_all("img"):
            src = img_tag.get("src")
            if src:
                images.add(src)

        # Correctly extend relative links
        images = self.normalize_url(url, images)
        return images


    def hash_html(self, html_content):
        return hashlib.sha256(html_content.encode("utf-8")).hexdigest()

    def get_domain(self, url):
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        return domain


    def worker(self, frontier, last_access_time, last_access_ips, stop_event):
        """Worker function for processes."""

        while not stop_event.is_set():
            try:
                frontier_response = requests.get("http://localhost:5000/frontier").json()
                page_id = frontier_response["id"]
                url = frontier_response["url"]
            except:
                print("Frontier is empty.")
                break
            
            print(f"Crawling URL: {url}")
            domain = self.get_domain(url)

            delay = requests.post("http://localhost:5000/site/delay", json={"site_url": domain, "ip": self.ip_address}).json()
            print(f"Delay: {delay['delay']}")
            time.sleep(delay["delay"])
            #self.respect_crawl_delay(domain, url, last_access_time, last_access_ips)

            print(f"Fetching: {url}")
            html, status_code, content_type = self.fetch(url)
            soup = BeautifulSoup(html, "html.parser")

            # remove scripts and css
            for script in soup.find_all("script"):
                script.decompose()

            # Remove all <style> tags
            for style in soup.find_all("style"):
                style.decompose()

            normalized_html = soup.prettify()
            page_hash = self.hash_html(normalized_html)
            links = self.extract_links(url, soup)
            images = self.extract_images(url, soup)

            try:
                requests.put("http://localhost:5000/page/" + str(page_id),
                             json={"page_type_code": "HTML", "html_content": html, "http_status_code": status_code,
                                   "accessed_ip": self.ip_address})
            except Exception as e:
                print(f"Error while updating page: {e}")
                continue

            #print(f"{multiprocessing.current_process().name} + {page_hash}")
            print(f'Found {len(links)} links and {len(images)} images.')
            # print(images)

            try:
                requests.post("http://localhost:5000/page/frontierlinks", json={"links": list(links)})
            except Exception as e:
                print(f"Error while updating frontier: {e}")
                continue



    def run(self, num_workers=2):
        args = (self.frontier, self.last_access_times, self.last_access_ips, self.stop_event)
        processes = [multiprocessing.Process(target=self.worker, args=args) for _ in range(num_workers)]

        for i, p in enumerate(processes):
            time.sleep(i*5)
            p.start()
        for p in processes:
            p.join()

        print("Crawling complete.")

# Run the crawler
if __name__ == '__main__':
    crawler = Crawler()
    crawler.run(num_workers=3)