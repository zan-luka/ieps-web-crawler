import multiprocessing
import time
from urllib.parse import urlsplit
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

options = Options()
options.add_argument("--headless=new")


class Crawler:
    def __init__(self):
        self.frontier = multiprocessing.Queue()
        self.frontier.put("https://fri.uni-lj.si/sl")  # Seed URL


    def read_robots(self, domain):
        robots_url = domain + "/robots.txt"
        robots_content = requests.get(robots_url).text
        return robots_content

    def read_sitemap(self, domain):
        sitemap_url = domain + "/sitemap.xml"
        sitemap_content = requests.get(sitemap_url).text
        return sitemap_content

    def fetch(self, url):
        driver = webdriver.Firefox(options=options)
        driver.get(url)
        page_source = driver.page_source
        driver.close()
        return page_source

    def determine_page_type(self, url):
        if url.endswith(".html"):
            return "HTML"
        elif url.endswith(".pdf"):
            return "PDF"
        elif url.endswith(".jpg") or url.endswith(".jpeg") or url.endswith(".png"):
            return "Image"
        else:
            return "Unknown"

    def normalize_url(self, base_url, links):
        normalized_links = set()
        for link in links:
            # Parse the link
            parsed_link = urlsplit(link)

            # If the link is relative, make it absolute
            if not parsed_link.scheme:
                link = base_url + link

            # Canonicalize the URL
            link = link.lower()
            if link.endswith('/'):
                link = link[:-1]

            normalized_links.add(link)

        return normalized_links


    def extract_links(self, url, html):
        soup = BeautifulSoup(html, "html.parser")
        links = set()
        for a_tag in soup.find_all("a"):
            href = a_tag.get("href")
            if href:
                links.add(href)

        # include links from onclick attributes
        for tag in soup.find_all():
            if tag.has_attr("onclick"):
                links.add(tag["onclick"])

        # correctly extend relative links
        links = self.normalize_url(url, links)
        return links

    def extract_images(self, url, html):
        soup = BeautifulSoup(html, "html.parser")
        images = set()
        for img_tag in soup.find_all("img"):
            src = img_tag.get("src")
            if src:
                images.add(src)

        # correctly extend relative links
        images = self.normalize_url(url, images)
        return images


    def worker(self, frontier):
        """Worker function for processes."""
        while not frontier.empty():
            try:
                url = frontier.get(timeout=3)
            except:
                break

            print(f"Fetching: {url}")
            html = self.fetch(url)
            page_type = self.determine_page_type(url)
            print(f"Page type: {page_type}")
            links = self.extract_links(url, html)
            images = self.extract_images(url, html)
            print(links)
            print(images)


    def run(self, num_workers=2):
        processes = [multiprocessing.Process(target=self.worker, args=(self.frontier,)) for _ in range(num_workers)]

        for p in processes:
            p.start()
        for p in processes:
            p.join()

        print("Crawling complete.")

# Run the crawler
if __name__ == '__main__':
    crawler = Crawler()
    crawler.run(num_workers=1)
