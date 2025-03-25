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

        seed_url = "https://slo-tech.com/"
        response = requests.post("http://localhost:5000/page/frontierlinks", json={"from_page_id": None,
                                                                                   "links": [{"url": seed_url, "relevance": 3}]})

        manager = multiprocessing.Manager()
        self.stop_event = manager.Event()

        self.max_pages = 1

    """
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
    """

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
            response = requests.get(sitemap_url, timeout=5)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            return [url.text.strip() for url in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")]
        except Exception as e:
            print(f"[ERROR] Failed to parse sitemap {sitemap_url}: {e}")
            return []

    def get_robots_txt(self, domain):
        robots_url = f"https://{domain}/robots.txt"
        try:
            return requests.get(robots_url, timeout=5).text
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
                sitemap_content += url + "\n"
                #if self.is_allowed(url):
                normalized_urls = self.normalize_url(domain, url)
                try:
                    requests.post("http://localhost:5000/page/frontierlinks", json={
                        "from_page_id": page_id,
                        "links": [{"url": norm_url, "relevance": 3} for norm_url in normalized_urls]
                    })
                except Exception as e:
                    print(f"[ERROR] Failed to enqueue links from sitemap: {e}")


    def get_or_create_site(self, domain, page_id):
        # Check if site already exists
        try:
            response = requests.get("http://localhost:5000/site/exists", params={"domain": domain})
            response.raise_for_status()
            data = response.json()
            if data.get("exists"):
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
            response = requests.post("http://localhost:5000/site", json={
                "domain": domain,
                "robots_content": robots_content,
                "sitemap_content": sitemap_content
            })
            response.raise_for_status()
            return response.json()["id"]
        except Exception as e:
            print(f"[ERROR] Failed to create site {domain}: {e}")
            return None

        except Exception as e:
            print(f"Napaka pri get_or_create_site: {e}")
            return None

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
    
    def check_relevance(self, links):
        relevant_links = []

        for link in links:
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

    def get_domain(self, url):
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        return domain

    def worker(self, stop_event):
        """Worker function for processes."""

        while not stop_event.is_set():
            html_response = requests.get("http://localhost:5000/page/html-count").json()
            html_count = html_response["html_page_count"]
            if html_count > self.max_pages:
                print(f"[{multiprocessing.current_process().name}] {self.max_pages} pages have been retrieved.")
                stop_event.set()
                break

            try:
                frontier_response = requests.get("http://localhost:5000/frontier").json()
                page_id = frontier_response["id"]
                url = frontier_response["url"]
            except:
                print("Frontier is empty.")
                break
            
            print(f"Crawling URL: {url}")
            domain = self.get_domain(url)
            site_id = self.get_or_create_site(domain, page_id)

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
                             json={"page_type_code": content_type, "html_content": html, "http_status_code": status_code,
                                   "accessed_ip": self.ip_address, "site_id": site_id})
            except Exception as e:
                print(f"Error while updating page: {e}")
                continue

            #print(f"{multiprocessing.current_process().name} + {page_hash}")
            print(f'Found {len(links)} links and {len(images)} images.')
            # print(images)

            relevant_links = self.check_relevance(links)

            try:
                response = requests.post("http://localhost:5000/page/frontierlinks", json={
                    "from_page_id": page_id,
                    "links": [{"url": url, "relevance": relevance} for url, relevance in relevant_links]
                })
                print(response.json)
            except Exception as e:
                print(f"Error while updating frontier: {e}")
                continue


    def run(self, num_workers=2):
        args = (self.stop_event,)
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
    crawler.run(num_workers=1)