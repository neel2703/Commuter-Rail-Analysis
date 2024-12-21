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
parser.add_argument('-p', '--project', type=str, help='BigQuery project ID', required=True)
parser.add_argument('-q', '--question_num', type=str, help='question for which the data needs to be uploaded', required=True)
args = parser.parse_args()

question_nums = ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8']

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

def extract_table_data(zip_url, table_names, season, year):
    response = requests.get(zip_url)
    zip_file = zipfile.ZipFile(BytesIO(response.content))
    # tables = {}
    for file_info in zip_file.infolist():
        if str(file_info.filename).split('.')[0] == table_names:
            with zip_file.open(file_info.filename) as file:
                df = pd.read_csv(file)
                table = df
    return table

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
    # start_year, end_year = map(int, args.year_range.split('-'))
    project_id = args.project
    seasons = ['Spring', 'Summer', 'Fall', 'Winter']

    db = get_db()
    client = bigquery.Client(project=project_id)
    dataset_id = f"analysis_data"
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    try:
        client.create_dataset(dataset)  # create dataset if it doesn't exist
    except Exception as e:
        print(f"Dataset {dataset_id} already exists.")

    if args.question_num == 'q1':
        from analysis_scripts import TripCount_TimeOfDay
        years = [19, 20, 21, 22, 23, 24]
        q1_table = TripCount_TimeOfDay.get_tripcount_weekday(years)
        table_data = q1_table
    
    if args.question_num == 'q2':
        from analysis_scripts import Q2_ExpressScript
        years = [19, 20, 21, 22, 23, 24]
        seasons = ['Spring', 'Summer', 'Fall', 'Winter']
        q2_table = Q2_ExpressScript.get_express_average_per_year(years, seasons)
        # print('q2: ', q2_table)
        table_data = q2_table

    if args.question_num == 'q3':
        from analysis_scripts import fare_zone_change
        table_names = ['stops']
        years = [2021, 2022, 2023, 2024]
        table_args = {}
        for year in years:
            for season in seasons:
                for table in table_names:
                    filtered_data = db[(db['season'] == season) & (db['year'] == str(year))]
                    if not filtered_data.empty:
                        try:
                            zip_url = filtered_data.iloc[0]['archive_url']
                            table_args[f'{table}_{year}_{season}'] = (extract_table_data(zip_url, table, season, year))
                        except Exception as e:
                            print(f'Table {table} not found for {season} {year}')
                            continue

        q3_table = fare_zone_change.final_table(table_args)
        table_data = q3_table

    if args.question_num == 'q4':
        from analysis_scripts import farecost
        table_names = ['fare_products', 'fare_leg_rules', 'routes']
        years = [2023, 2024]
        table_args = {}
        for year in years:
            for season in seasons:
                for table in table_names:
                    filtered_data = db[(db['season'] == season) & (db['year'] == str(year))]
                    if not filtered_data.empty:
                        try:
                            zip_url = filtered_data.iloc[0]['archive_url']
                            table_args[f'{table}_{year}_{season}'] = (extract_table_data(zip_url, table, season, year))
                        except Exception as e:
                            print(f'Table {table} not found for {season} {year}')
                            continue
        
        q4_table = farecost.main(table_args)
        table_data = q4_table

    if args.question_num == 'q5' or args.question_num == 'q7':
        from analysis_scripts import schedule
        q5_table = schedule.main()
        q7_table = schedule.main()
        # print('q5: ', q5_table)
        table_data = q5_table

    if args.question_num == 'q6':
        from analysis_scripts import Net_num_of_trains
        q6_table = Net_num_of_trains.net_trains_per_line
        # print('q6: ', q6_table)
        table_data = q6_table

    table_data = convert_column_types(table_data)
    table_data_dict = table_data.to_dict(orient='records')
    upload_to_bigquery(table_data, project_id, dataset_id, args.question_num)
    print(f"Uploaded {args.question_num} to BigQuery dataset {dataset_id}.")