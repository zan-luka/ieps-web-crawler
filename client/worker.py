from urllib.parse import urlsplit
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

options = Options()
options.add_argument("--headless=new")

class Crawler:
    def __init__(self, url):
        self.url = url
        self.domain = "{0.scheme}://{0.netloc}".format(urlsplit(url))
        self.visited = set()
        self.driver = webdriver.Firefox(options=options)

    def parse(self, html):
        pass


    def fetch(self):
        self.driver.get(self.url)
        return self.driver.page_source


    # function that reads the robots.txt file of a website
    def read_robots(self):
        robots_url = self.domain + "/robots.txt"
        robots_content = requests.get(robots_url).text
        return robots_content

    def read_sitemap(self):
        sitemap_url = self.domain + "/sitemap.xml"
        sitemap_content = requests.get(sitemap_url).text
        return sitemap_content

    def crawl(self, url):
        if url in self.visited:
            return
        self.visited.add(url)
        print("Crawling URL %s" % url)
        soup = BeautifulSoup(requests.get(url).text, "html.parser")
        for link in soup.find_all("a"):
            href = link.get("href")
            if href is None:
                continue
            if href.startswith("/"):
                href = self.domain + href
            if not self.domain in href:
                continue
            self.crawl(href)