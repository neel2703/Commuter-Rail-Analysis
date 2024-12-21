import requests
from bs4 import BeautifulSoup
import pandas as pd
import zipfile
import os
from io import BytesIO
import re
from io import StringIO
import argparse
from itertools import product

parser = argparse.ArgumentParser()
parser.add_argument('-s', '--seasons', nargs='+', help='a list of the seasons', required=True)    
parser.add_argument('-y', '--years', nargs='+', help='a list of the years', required=True)
parser.add_argument('-t', '--tables', nargs='+', help='a list of the tables', required=True)
args = parser.parse_args()

url = "https://cdn.mbta.com/archive/archived_feeds.txt"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

def get_db():
    data = pd.read_csv(StringIO(soup.get_text()))
    data['season'] = data['feed_version'].apply(lambda x: re.search(r'(Fall|Spring|Summer|Winter)', x).group(0) if re.search(r'(Fall|Spring|Summer|Winter)', x) else None)
    data['year'] = data['feed_version'].apply(lambda x: re.search(r'\b(20\d{2})\b', x).group(0) if re.search(r'\b(20\d{2})\b', x) else None)
    return data

def extract_tables(zip_url, table_names, output_dir = './'):
    response = requests.get(zip_url)
    zip_file = zipfile.ZipFile(BytesIO(response.content))
    for file_info in zip_file.infolist():
        if str(file_info.filename).split('.')[0] in table_names:
            zip_file.extract(file_info, output_dir)

def download_datasets(db, season, year, tables):
    filtered_data = db[(db['season'] == str(season)) & (db['year'] == str(year))]
    zip_url = filtered_data.iloc[0]['archive_url']
    extract_tables(zip_url, tables, f'./datasets/{season}_{year}/')

if __name__ == '__main__':
    seasons = args.seasons
    years = args.years
    tables = args.tables
    
    db = get_db()

    for season, year in product(seasons, years):
        print(season, year)
        download_datasets(db, season, year, tables)
