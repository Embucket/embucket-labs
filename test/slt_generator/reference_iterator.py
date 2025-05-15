from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import requests
import time


class SnowflakeDocumentationIterator:
    def __init__(self):
        self.visited = set()

    def fetch_page(self, url):
        """Fetch a webpage and return its HTML content."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch {url}: {e}")
            return None

    def get_sublinks(self, url, soup):
        """Extract sublinks from a page."""
        sub_links = []
        for link in soup.select('div#selected-item + ul a'):
            href = link.get("href")
            if href:
                full_url = urljoin(url, href)
                sub_links.append(full_url)
        return list(sub_links)

    def process_page(self, url, category_name, subcategory_name=None):
        """Process a page and its subpages."""
        if url in self.visited:
            return

        print(f"Processing: {url}")
        html = self.fetch_page(url)
        if not html:
            return

        soup = BeautifulSoup(html, "html.parser")
        sub_links = self.get_sublinks(url, soup)

        current_subcategory_name = subcategory_name
        if sub_links:
            current_subcategory_name = subcategory_name or urlparse(url).path.split("/")[-1] or None
            if not current_subcategory_name or current_subcategory_name == category_name:
                current_subcategory_name = None

        self.visited.add(url)

        # Call the handler method that will be implemented by subclasses
        self.handle_page(html, url, category_name, current_subcategory_name)

        for sub_link in sub_links:
            self.process_page(sub_link, category_name, current_subcategory_name)

    def handle_page(self, html, url, category_name, subcategory_name):
        """Override this method in subclasses to implement specific page handling."""
        raise NotImplementedError

    def run(self):
        root_urls = [
            # "https://docs.snowflake.com/en/sql-reference-commands",
            "https://docs.snowflake.com/en/sql-reference-functions"
        ]

        for root_url in root_urls:
            category_name = root_url.split("/")[-1]
            self.process_page(root_url, category_name)