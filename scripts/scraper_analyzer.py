import os
import sys
import pandas as pd
from jobspy import scrape_jobs
from langdetect import detect
from utils.check_proxies import ProxiesChecker

# base version, full codes: https://github.com/LeonSavi/job-scraper/blob/master/scripts/scraper_analyzer.py

class JobAnalyzer():

    def __init__(self,
                 configs: dict,
                 use_proxies:bool = False):
        
        for key, value in configs.items():
            setattr(self, key, value)

        self.proxies = ProxiesChecker().get_valid_proxies() if use_proxies else []

    def scrape_jobs(self):
        # print('--- SCRAPING ---', file=sys.stderr)

        jobs = scrape_jobs(
            site_name=["indeed"],
            search_term=self.search_term,
            results_wanted=self.n_results,
            hours_old=self.hours_old,
            country_indeed=self.country,
            location=self.location,
            proxies=["localhost"] + self.proxies
        )

        jobs['scrape_time'] = pd.Timestamp.now().strftime("%d-%m-%Y")
        jobs['lang_descr'] = jobs['description'].apply(self._detect_lang)

        return jobs

    def _detect_lang(self, text):
        try:
            return detect(text)
        except:
            return None