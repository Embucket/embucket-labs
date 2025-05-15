import os
import time
from datetime import datetime
from urllib.parse import urlparse
from reference_iterator import SnowflakeDocumentationIterator


class SnowflakeDocumentationDownloader(SnowflakeDocumentationIterator):
    def __init__(self, base_folder="documentation-snapshot"):
        super().__init__()
        self.base_folder = base_folder

    def handle_page(self, html, url, category_name, subcategory_name):
        """Save the page source to a file."""
        filename = urlparse(url).path.split("/")[-1]
        filepath = f"{self.base_folder}/{category_name}/"
        if subcategory_name:
            filepath += f"{subcategory_name}/"
        filepath += f"{filename}.html"

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"Saved: {url} → {filepath}")
        time.sleep(0.5)  # Avoid excessive requests

    def save_snapshot(self):
        """Save a snapshot log after processing all pages."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        snapshot_file = os.path.join(self.base_folder, "snapshot.txt")

        with open(snapshot_file, "w", encoding="utf-8") as f:
            f.write(f"Snapshot taken at: {timestamp}\n")

        print(f"Snapshot saved: {snapshot_file}")

    def run(self):
        super().run()
        self.save_snapshot()


if __name__ == "__main__":
    downloader = SnowflakeDocumentationDownloader()
    downloader.run()