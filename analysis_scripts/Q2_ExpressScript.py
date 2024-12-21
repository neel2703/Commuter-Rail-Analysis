# the get_express_average_per_year(years, seasons, trips_data, stop_times_data, stops_data)
# function will return a table containing the number of express train for each line

import requests
from bs4 import BeautifulSoup
from io import BytesIO
from io import StringIO
import re
import zipfile
import pandas as pd
import os
import matplotlib.pyplot as plt
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
# import bigquery_cleaned_pipeline as bcp


years = [19, 20, 21, 22, 23, 24]
seasons = ['Spring', 'Summer', 'Fall', 'Winter']
lines_data = {}
routes_data = {}
trips_data = {}
calendar_data = {}
stop_times_data = {}

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

table_names = ['lines', 'routes', 'trips', 'calendar', 'stop_times']
table_args = {}
db = get_db()
for year in years:
    for season in seasons:
        for table in table_names:
            filtered_data = db[(db['season'] == season) & (db['year'] == f'20{year}')]
            if not filtered_data.empty:
                try:
                    zip_url = filtered_data.iloc[0]['archive_url']
                    table_args[f'{table}_{year}{season}'] = (extract_table_data(zip_url, table, season, year))
                except Exception as e:
                    print(f'Table {table} not found for {season} {year}')
                    continue

#Read the data
for year in years:
    for season in seasons:
        try:
            lines_data[f'lines_{year}{season}'] = table_args[f'lines_{year}{season}'].drop(columns=['line_desc', 'line_url', 'line_short_name'], errors='ignore')
            globals()[f'lines_{year}{season}'] = lines_data[f'lines_{year}{season}']

            routes_data[f'routes_{year}{season}'] = table_args[f'routes_{year}{season}']
            globals()[f'routes_{year}{season}'] = table_args[f'routes_{year}{season}']
            
            trips_data[f'trips_{year}{season}'] = table_args[f'trips_{year}{season}']
            globals()[f'trips_{year}{season}'] = table_args[f'trips_{year}{season}']
            
            calendar_data[f'calendar_{year}{season}'] = table_args[f'calendar_{year}{season}']
            globals()[f'calendar_{year}{season}'] = table_args[f'calendar_{year}{season}']
            
            stop_times_data[f'stop_times_{year}{season}'] = table_args[f'stop_times_{year}{season}']
            globals()[f'stop_times_{year}{season}'] = table_args[f'stop_times_{year}{season}']
        except Exception as e:
            continue
 
 
cr_line = {}
filtered_list = []

for year in years:
    for season in seasons:
        try:
            current_trips = trips_data[f'trips_{year}{season}']
            
            cr_trips = current_trips[current_trips['route_id'].str.startswith('CR-')]
            trips_data[f'trips_{year}{season}'] = cr_trips
            globals()[f'trips_{year}{season}'] = cr_trips 
        except Exception as e:
            continue 

def get_global_variable(name):
    return globals().get(name, None)

def analyze_express_trains(year, season):
    identifier = f'{year}{season}'
    
    # get data
    stop_times = get_global_variable(f'stop_times_{identifier}')
    trips = get_global_variable(f'trips_{identifier}')
    routes = get_global_variable(f'routes_{identifier}')
    
    if stop_times is None or trips is None or routes is None:
        print(f"Missing data for {identifier}")
        return None
    
    # stop num for each trip
    trip_stop_counts = stop_times.groupby('trip_id').stop_id.nunique().reset_index()
    trip_stop_counts.rename(columns={'stop_id': 'stop_count'}, inplace=True)
    
    # merge trips and routes
    trip_data = trips.merge(routes, on='route_id')
    trip_data = trip_data.merge(trip_stop_counts, on='trip_id')
    
    # max stop num
    max_stops = trip_data.groupby('route_id')['stop_count'].max().reset_index()
    max_stops.rename(columns={'stop_count': 'max_stop_count'}, inplace=True)
    trip_data = trip_data.merge(max_stops, on='route_id')
    
    # label Express Train
    trip_data['is_express'] = trip_data['stop_count'] < trip_data['max_stop_count']
    
    # get Express Train num
    express_trains = trip_data[trip_data['is_express']].groupby('route_id').size().reset_index(name='express_train_count')
    return express_trains


def get_express_average_per_year(years, seasons, trips_data=trips_data, stop_times_data=stop_times_data):
    """
    Calculate the average number of express trains for each route for each year, 
    considering all seasons (spring, summer, fall, winter) for each year.

    Args:
        years (list): List of years to analyze.
        seasons (list): List of seasons to analyze (e.g., ['winter', 'spring', 'summer', 'fall']).
        trips_data (dict): Dictionary containing trips data for each year and season.
        stop_times_data (dict): Dictionary containing stop_times data for each year and season.
        stops_data (dict): Dictionary containing stops data for each year and season.

    Returns:
        DataFrame: Average number of express trains per route_id for each year, considering all seasons.
    """
    all_season_express_counts = []  # To store express train counts for all seasons

    # Loop over years and seasons
    for year in years:
        season_express_counts = []  # Store counts for each season for a specific year
        
        for season in seasons:
            try:
                print(f"Analyzing {year} {season}...")

                # Retrieve the data for the current year and season
                trips = trips_data[f'trips_{year}{season}']
                stop_times = stop_times_data[f'stop_times_{year}{season}']
                # stops = stops_data[f'stops_{year}{season}']

                # Merge trips and stop_times data on trip_id
                merged_data = pd.merge(trips, stop_times, on='trip_id', how='inner')
                
                # Calculate the gap between consecutive stops for each trip
                merged_data['next_stop_gap'] = merged_data.groupby('trip_id')['stop_sequence'].diff().fillna(1)
                
                # Identify express trains where the gap between stops is greater than 1
                merged_data['is_express'] = merged_data['next_stop_gap'] > 1
                
                # Filter only express train data
                express_trains = merged_data[merged_data['is_express']]
                
                # Group by route_id and count unique express train trips
                express_train_counts = express_trains.groupby('route_id')['trip_id'].nunique().reset_index()
                express_train_counts.columns = ['route_id', 'express_train_count']  # Rename columns for clarity
                express_train_counts['year'] = year  # Add the year to each entry
                
                # Append the seasonal results to the list for this season
                season_express_counts.append(express_train_counts)
            except Exception as e:
                print(f"No data to analyze {year} {season}")
                continue

        # After all seasons are processed for a year, calculate the average for each route_id
        year_express_counts = pd.concat(season_express_counts, ignore_index=True)

        # Calculate the average number of express trains per route for the year across all seasons
        average_express_counts_per_year = (
            year_express_counts.groupby('route_id')['express_train_count']
            .mean()  # Take the mean across all seasons
            .reset_index()
        )
        average_express_counts_per_year['year'] = year  # Add the year to the results

        all_season_express_counts.append(average_express_counts_per_year)

    # Combine all yearly data into a single DataFrame
    final_average_express_counts = pd.concat(all_season_express_counts, ignore_index=True)
    
    return final_average_express_counts


def plot_express(final_express_counts):
    final_express_counts['year'] = final_express_counts['year'].apply(lambda x: 2000 + x if x < 100 else x)

    final_express_counts = final_express_counts[(final_express_counts['year'] >= 2021) & (final_express_counts['year'] <= 2024)]

    final_express_counts = final_express_counts.sort_values(by='year')

    # plt.figure(figsize=(10, 6))
    fig, ax = plt.subplots(figsize=(10, 6))

    for route in final_express_counts['route_id'].unique():
        subset = final_express_counts[final_express_counts['route_id'] == route]
        ax.plot(subset['year'].values, subset['express_train_count'].values, marker='o', label=route)

    ax.set_title('Express Train Count by Year for Different Routes')
    ax.set_xlabel('Year')
    ax.set_ylabel('Express Train Count')
    ax.legend(title='Route ID', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(True)
    ax.set_xticks(range(2021, 2025))
    plt.tight_layout()

    return fig