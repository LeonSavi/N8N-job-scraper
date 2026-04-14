import os
import re
import pandas as pd
import json
from time import sleep
from scripts.scraper_analyzer import JobAnalyzer
import argparse
import sys
import random
from utils.helper_functions import clean_description, parse_locations


parser = argparse.ArgumentParser()
parser.add_argument('--countries', default='netherlands,belgium,germany,italy')
parser.add_argument('--search_term', default='data scientist,data engineer')
parser.add_argument('--locations',default='amsterdam (netherlands), utrecht (netherlands), berlin (germany)')
parser.add_argument('--n_results', type=int, default=50)
parser.add_argument('--hours_old', type=int, default=24)
parser.add_argument('--chat_id', default='default')
args = parser.parse_args()


OUTPUT_FOLDER = 'outputs'
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, f'job_search_{args.chat_id}.csv')
SENT_FILE = os.path.join(OUTPUT_FOLDER, f'sent_ids_{args.chat_id}.txt')
FILE_CHECK = os.path.isfile(OUTPUT_FILE)

SEARCH_TERMS = [c.strip() for c in args.search_term.split(',')]
COUNTRIES = [c.strip() for c in args.countries.split(',')]
LOCATIONS = parse_locations(args.locations,COUNTRIES)

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def load_sent():
    if not os.path.exists(SENT_FILE):
        return set()
    return set(open(SENT_FILE).read().splitlines())

def mark_sent(urls):
    with open(SENT_FILE, 'a') as f:
        f.write('\n'.join(urls) + '\n')

if __name__ == '__main__':
    results = []
    sent = load_sent()

    for curr_country in COUNTRIES:
        for curr_search_term in SEARCH_TERMS:
            for curr_city in LOCATIONS[curr_country]:
                configs = {
                    'country': curr_country,
                    'search_term': curr_search_term,   
                    'n_results': args.n_results,        
                    'hours_old': args.hours_old,
                    'location': curr_city
                }

                scraper = JobAnalyzer(configs)
                out = scraper.scrape_jobs()
                out['ref_country'] = curr_country
                out['search_term'] = curr_search_term
                results.append(out)
                sleep(random.randint(2,8))

    if not results:
        print(json.dumps([]))
        sys.exit(0)

    df = pd.concat(results, ignore_index=True).reset_index(drop=True)
    df.fillna('',inplace=True)
    df.to_csv(OUTPUT_FILE, mode='a', index=False, header=not FILE_CHECK)

    output_cols = ['title', 'company', 
                'location', 'job_url',
                'job_url_direct', 'search_term',
                'description', 'ref_country', 'date_posted',
                'lang_descr']
    
    available_cols = [c for c in output_cols if c in df.columns]

    clean = (
        df[~df['job_url'].isin(sent)]
        [available_cols]
        .dropna(subset=['job_url'])
        .drop_duplicates(subset=['job_url'])
    )

    if 'description' in clean.columns:
        clean = clean.copy()
        clean['description'] = clean['description'].apply(clean_description)

    mark_sent(clean['job_url'].tolist())
    print(json.dumps(clean.to_dict(orient='records'), default=str))