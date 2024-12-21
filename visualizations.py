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
import matplotlib.pyplot as plt
import sys

parser = argparse.ArgumentParser()
parser.add_argument('-q', '--question_num', type=str, help='question for which the visualization is needed', required=True)
args = parser.parse_args()

question_nums = ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7']

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


if __name__ == '__main__':
    seasons = ['Spring', 'Summer', 'Fall', 'Winter']

    db = get_db()

    if args.question_num == 'q1':
        from analysis_scripts import TripCount_TimeOfDay
        years = [19, 20, 21, 22, 23, 24]
        q1_table = TripCount_TimeOfDay.get_tripcount_weekday(years)
        table_data = q1_table
        plot = TripCount_TimeOfDay.get_plot(table_data)

    if args.question_num == 'q2':
        from analysis_scripts import Q2_ExpressScript
        years = [19, 20, 21, 22, 23, 24]
        seasons = ['Spring', 'Summer', 'Fall', 'Winter']
        q2_table = Q2_ExpressScript.get_express_average_per_year(years, seasons)
        table_data = q2_table
        plot = Q2_ExpressScript.plot_express(table_data)

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
        plot = fare_zone_change.plot_heatmap(table_data)

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
        # table_data = q4_table
        print(f'No visualization available for {args.question_num}')
        sys.exit()

    if args.question_num == 'q5' or args.question_num == 'q7':
        from analysis_scripts import schedule
        q5_table = schedule.main()
        # q7_table = schedule.main()
        table_data = q5_table
        plot = schedule.plot_all_routes_chronological(table_data)

    if args.question_num == 'q6':
        from analysis_scripts import Net_num_of_trains
        q6_table = Net_num_of_trains.net_trains_per_line
        plot = Net_num_of_trains.fig_net_trains

    plot.savefig(f'visualizations/{args.question_num}.png', dpi=300, bbox_inches='tight', pad_inches=0.1)
    print(f'Visualization for {args.question_num} saved at visualizations/{args.question_num}.png')
    plt.close(plot)