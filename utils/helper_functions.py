from collections import defaultdict
import re
import os

def parse_locations(location_str: str, all_countries:str) -> dict:
    """Returns {country: [city1, city2]} or {country: [None]} if no cities
    all_countries are those from CONTRIES"""

    result = defaultdict(list)

    for item in location_str.split(','):
        item = item.strip()
        if '(' in item and ')' in item:
            city = item[:item.index('(')].strip().lower()
            country = item[item.index('(')+1:item.index(')')].strip().lower()

            result[country].append(city)

    for country in all_countries:
        if country not in result.keys():
            result[country].append(None)

    return result


def escape_markdown(text: str) -> str:
    "for Markdown in n8n"
    if not isinstance(text, str):
        return ''
    text = re.sub(r'[_*`\[]', '', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip()


def clean_description(text: str, max_len: int = 400) -> str:
    if not isinstance(text, str):
        return ''
    
    text = re.sub(r'\\(.)', r'\1', text)         
    text = re.sub(r'\*+', '', text)               
    text = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', text)  
    text = re.sub(r'-{2,}', '', text)             
    text = re.sub(r'#{1,6}\s*', '', text)         
    text = re.sub(r'\n+', ' ', text)              
    text = re.sub(r'\s{2,}', ' ', text)           
    
    return text.strip()[:max_len] + '...'


