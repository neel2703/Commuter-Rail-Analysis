import requests
from bs4 import BeautifulSoup
import pandas as pd
import zipfile
import os
from io import BytesIO
from io import StringIO
import re
from google.cloud import bigquery
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-y', '--year_range', type=str, help='Year range in the format "start-end"', required=True)
parser.add_argument('-p', '--project', type=str, help='BigQuery project ID', required=True)
args = parser.parse_args()

url = "https://cdn.mbta.com/archive/archived_feeds.txt"
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

def get_db():
    data = pd.read_csv(StringIO(soup.get_text()))
    data['season'] = data['feed_version'].apply(
        lambda x: re.search(r'(Fall|Spring|Summer|Winter)', x).group(0) if re.search(r'(Fall|Spring|Summer|Winter)', x) else None
    )
    data['year'] = data['feed_version'].apply(
        lambda x: re.search(r'\b(20\d{2})\b', x).group(0) if re.search(r'\b(20\d{2})\b', x) else None
    )
    return data

def extract_table_data(zip_url):
    response = requests.get(zip_url)
    zip_file = zipfile.ZipFile(BytesIO(response.content))
    tables = {}
    for file_info in zip_file.infolist():
        file_name = file_info.filename
        if file_name.endswith('.txt'):
            with zip_file.open(file_name) as file:
                table_name = os.path.splitext(file_name)[0]
                df = pd.read_csv(file)
                if table_name in tables:
                    tables[table_name] = pd.concat([tables[table_name], df], ignore_index=True)
                else:
                    tables[table_name] = df
    return tables

def convert_column_types(df):
    for column in df.columns:
        df[column] = df[column].astype(str)
    return df

def upload_to_bigquery(dataframe, project_id, dataset_name, table_name):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    job = client.load_table_from_dataframe(dataframe, table_ref)
    job.result()  # wait for job to complete

if __name__ == '__main__':
    start_year, end_year = map(int, args.year_range.split('-'))
    project_id = args.project

    db = get_db()
    client = bigquery.Client(project=project_id)
    for year in range(start_year, end_year + 1):
        dataset_id = f"{year}_data"
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        try:
            client.create_dataset(dataset)  # create dataset if it doesn't exist
        except Exception as e:
            print(f"Dataset {dataset_id} already exists.")

        # merge the tables
        merged_tables = {}
        for season in ['Fall', 'Spring', 'Summer', 'Winter']:
            filtered_data = db[(db['season'] == season) & (db['year'] == str(year))]
            if not filtered_data.empty:
                zip_url = filtered_data.iloc[0]['archive_url']
                tables = extract_table_data(zip_url)
                for table_name, table_data in tables.items():
                    table_data['year'] = str(year)
                    table_data['season'] = season
                    if table_name in merged_tables:
                        merged_tables[table_name] = pd.concat([merged_tables[table_name], table_data], ignore_index=True)
                    else:
                        merged_tables[table_name] = table_data

        # uploading the merged tables to bigquery
        for table_name, table_data in merged_tables.items():
            table_data = convert_column_types(table_data)
            upload_to_bigquery(table_data, project_id, dataset_id, table_name)
            print(f"Uploaded {table_name} for year {year} to BigQuery dataset {dataset_id}.")

