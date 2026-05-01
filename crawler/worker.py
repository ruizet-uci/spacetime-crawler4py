from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        tokens_dict = scraper.initialize_buffer("token_counts.txt")
        top_record = scraper.initialize_longest_doc("longest_doc.txt")
        urls_dict = scraper.initialize_url_buffer("unique_urls.txt")
        subdomain_dict = scraper.initialize_subdomain_buffer("subdomains.txt")
        robot_rules_dict = {}
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            try:
                scraped_urls = scraper.scraper(tbd_url, resp, tokens_dict, top_record, urls_dict, subdomain_dict, self.config, self.logger, robot_rules_dict)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
            except scraper.OverwhelmedException:
                pass
