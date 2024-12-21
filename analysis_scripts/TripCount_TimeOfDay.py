# This is the python file for Trip number by time of day

# First table: The function trip_count_by_year(years, seasons, stop_times_data, trips_data, weekday_services) returns a dataframe
#   a data frame trip_count, which contains the number of trip by time of day of each line

# Second table: The function get_tripcount_weekday(years) will return a trip_count that contains the number of trips 
#   in different time period by weekday


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
import seaborn as sns
# parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# sys.path.append(parent_dir)
# import bigquery_cleaned_pipeline as bcp

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

years = [19, 20, 21, 22, 23, 24]
seasons = ['Spring', 'Summer', 'Fall', 'Winter']
lines_data = {}
routes_data = {}
trips_data = {}
calendar_data = {}
stop_times_data = {}

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
        # file_path = f'/Users/lto/Desktop/MBTA_19-24/{year}{season}/lines.txt'
        # lines_data[f'lines_{year}{season}'] = pd.read_csv(file_path, delimiter=',')
            lines_data[f'lines_{year}{season}'] = table_args[f'lines_{year}{season}'].drop(columns=['line_desc', 'line_url', 'line_short_name'], errors='ignore')
            globals()[f'lines_{year}{season}'] = lines_data[f'lines_{year}{season}']
        # file_path = f'/Users/lto/Desktop/MBTA_19-24/{year}{season}/routes.txt'
        # routes_data[f'routes_{year}{season}'] = pd.read_csv(file_path, delimiter=',')
            routes_data[f'routes_{year}{season}'] = table_args[f'routes_{year}{season}']
            globals()[f'routes_{year}{season}'] = table_args[f'routes_{year}{season}']
            
            # file_path = f'/Users/lto/Desktop/MBTA_19-24/{year}{season}/trips.txt'
            # trips_data[f'trips_{year}{season}'] = pd.read_csv(file_path, delimiter=',')
            trips_data[f'trips_{year}{season}'] = table_args[f'trips_{year}{season}']
            globals()[f'trips_{year}{season}'] = table_args[f'trips_{year}{season}']
            
            # file_path = f'/Users/lto/Desktop/MBTA_19-24/{year}{season}/calendar.txt'
            # calendar_data[f'calendar_{year}{season}'] = pd.read_csv(file_path, delimiter=',')
            calendar_data[f'calendar_{year}{season}'] = table_args[f'calendar_{year}{season}']
            globals()[f'calendar_{year}{season}'] = table_args[f'calendar_{year}{season}']
            
            # file_path = f'/Users/lto/Desktop/MBTA_19-24/{year}{season}/stop_times.txt'
            # stop_times_data[f'stop_times_{year}{season}'] = pd.read_csv(file_path, delimiter=',')
            stop_times_data[f'stop_times_{year}{season}'] = table_args[f'stop_times_{year}{season}']
            globals()[f'stop_times_{year}{season}'] = table_args[f'stop_times_{year}{season}']
        except Exception as e:
            continue
        
        
trip_counts = pd.DataFrame()

def time_of_day(time_str):
    hour = int(time_str.split(':')[0]) 
    
    if 3 <= hour < 6:
        return 'Sunrise'
    elif 6 <= hour < 7:
        return 'Early AM'
    elif 7 <= hour < 9:
        return 'AM Peak'
    elif 9 <= hour < 13.5:
        return 'Midday Base'
    elif 13.5 <= hour < 16:
        return 'Midday School'
    elif 16 <= hour < 18.5:
        return 'PM Peak'
    elif 18.5 <= hour < 22:
        return 'Evening'
    elif 22 <= hour < 24:
        return 'Late Evening'
    else:
        return 'Night'
 
def fix_arrival_time(time_str):
    try:
        hours, minutes, seconds = map(int, time_str.split(":"))
        if hours >= 24:
            hours -= 24
            return f"{hours:02}:{minutes:02}:{seconds:02}"
        return time_str
    except ValueError:
        return None

for season in seasons:
    stop_times = stop_times_data.get(f'stop_times_24{season}')
    if stop_times is not None:
        stop_times['arrival_time'] = stop_times['arrival_time'].apply(fix_arrival_time)
  

def get_weekday_services(years, seasons, calendar_data):
    weekday_services = set()
    for year in years:
        for season in seasons:
            calendar_key = f'calendar_{year}{season}'
    
            if calendar_data is not None and calendar_key in calendar_data:
                calendar_df = calendar_data[calendar_key]
                weekdays = calendar_df[(calendar_df['monday'] == 1) |
                                    (calendar_df['tuesday'] == 1) |
                                    (calendar_df['wednesday'] == 1) |
                                    (calendar_df['thursday'] == 1) |
                                    (calendar_df['saturday'] == 1) |
                                    (calendar_df['sunday'] == 1) |
                                    (calendar_df['friday'] == 1)]
                weekday_services.update(weekdays['service_id'].unique())
            else:
                print(f'No calendar data for {year}{season} or calendar_data is None')
                
    return weekday_services

def trip_count_by_year(years, seasons, stop_times_data, trips_data, weekday_services):
    trip_counts = pd.DataFrame()
    for year in years:
        for season in seasons:
            stop_times_key = f'stop_times_{year}{season}'
            trips_key = f'trips_{year}{season}'
            if stop_times_key in stop_times_data and trips_key in trips_data:
                stop_times = stop_times_data[stop_times_key]
                trips = trips_data[trips_key]

                stop_times['time_period'] = stop_times['arrival_time'].apply(time_of_day)
                merged_data = stop_times.merge(trips[['trip_id', 'route_id', 'block_id', 'service_id']], on='trip_id')
                
                # keep only weekday
                merged_data = merged_data[merged_data['service_id'].isin(weekday_services)]

                # count number of trips 
                grouped = merged_data.groupby(['time_period', 'route_id']).size().reset_index(name='Trip Count')

                # add year and season
                grouped['Year'] = year
                grouped['Season'] = season
                
                trip_counts = pd.concat([trip_counts, grouped], ignore_index=True)
            else:
                print(f'No data for {stop_times_key} or {trips_key}')

    return trip_counts

# Q2: Number of trip in time of day by weekday and weekend
#
# Merge Trip and Calendar
tripcalendar_data = {}
for year in years:
    try:
        trips_df = pd.concat([trips_data[f'trips_{year}{season}'] for season in seasons], ignore_index=True)
        calendar_df = pd.concat([calendar_data[f'calendar_{year}{season}'] for season in seasons], ignore_index=True)
        
        tripcalendar_data[f'tripcalendar_{year}'] = pd.merge(trips_df, calendar_df, on='service_id', how='left')

        globals()[f'tripcalendar_{year}'] = tripcalendar_data[f'tripcalendar_{year}']
    except Exception as e:
        continue

tripcalendar_data = {}
for year in years:
    try:
        trips_df = pd.concat([trips_data[f'trips_{year}{season}'] for season in seasons], ignore_index=True)
        calendar_df = pd.concat([calendar_data[f'calendar_{year}{season}'] for season in seasons], ignore_index=True)
        
        tripcalendar_data[f'tripcalendar_{year}'] = pd.merge(trips_df, calendar_df, on='service_id', how='left')

        globals()[f'tripcalendar_{year}'] = tripcalendar_data[f'tripcalendar_{year}']
    except Exception as e:
        continue

def get_mergeddata(year):
    for season in seasons:
        stop_times = stop_times_data.get(f'stop_times_{year}{season}')
        if stop_times is not None:
            stop_times['time_period'] = stop_times['arrival_time'].apply(time_of_day)
            
            trips = trips_data.get(f'trips_{year}{season}')
            if trips is not None:
                merged_data = stop_times.merge(trips[['trip_id', 'service_id', 'route_id', 'block_id']], on='trip_id')
                
                calendar = calendar_data.get(f'calendar_{year}{season}')
                if calendar is not None:
                    trip_calendar_stoptime_data = merged_data.merge(
                        calendar[['service_id', 'monday', 'tuesday', 
                                  'start_date', 'end_date',
                                  'wednesday', 'thursday', 'friday', 'saturday', 'sunday']],
                        on='service_id',  
                        how='left'  
                    )
                    
    return trip_calendar_stoptime_data



def get_tripcount_weekday(years):
    l = []
    for year in years: 
        try:
            merged = get_mergeddata(year)
            l.append(merged)
        except Exception as e:
            continue
    columns_to_drop = ['arrival_time', 'departure_time', 'stop_sequence', 
                    'checkpoint_id', 
                    'stop_headsign', 'pickup_type', 'drop_off_type']
    all_merged_data = []
    for i in l:
        i['time_period'] = i['arrival_time'].apply(time_of_day)
        i= i.drop(columns=columns_to_drop)
        all_merged_data.append(i)
    final_data = pd.concat(all_merged_data, ignore_index=True)
    final_data['start_date'] = pd.to_datetime(final_data['start_date'], format='%Y%m%d')
    final_data['end_date'] = pd.to_datetime(final_data['end_date'], format='%Y%m%d')


        
    # Define lists for weekdays, weekends, and commuter rail lines
    inweekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekend = ['Saturday', 'Sunday']
    lines = [
        'CR-Fairmount', 'CR-Fitchburg', 'CR-Franklin','CR-Greenbush',
        'CR-Worcester',  'CR-Haverhill', 
        'CR-Kingston', 'CR-Lowell', 'CR-Middleborough', 
        'CR-Needham','CR-Newburyport','CR-Providence', 
        
    ]
    # Convert date columns to datetime if not already done
    final_data['start_date'] = pd.to_datetime(final_data['start_date'], format='%Y%m%d')
    final_data['end_date'] = pd.to_datetime(final_data['end_date'], format='%Y%m%d')

    # Initialize a dictionary to store results
    trip_counts = {}

    # Iterate through each line
    for line in lines:
        # Filter for the current line
        line_data = final_data[final_data['route_id'] == line]
        
        # Initialize a dictionary for each line to hold trip counts by day and time period
        trip_counts[line] = {}
        
        # Loop through weekdays and weekend days
        for day in inweekdays + weekend:
            # Filter trips that operate on the specific day
            day_data = line_data[line_data[day.lower()] == 1]
            
            # Group by time period and count trips
            trip_count_by_time_period = day_data.groupby('time_period')['trip_id'].count()
            
            # Store the result in the dictionary
            trip_counts[line][day] = trip_count_by_time_period.to_dict()

    # Convert to DataFrame if needed
    trip_counts_df = pd.DataFrame.from_dict({(i,j): trip_counts[i][j] 
                            for i in trip_counts.keys() 
                            for j in trip_counts[i].keys()},
                        orient='index')
    trip_counts_df.index.names = ['Line', 'Day']
    trip_counts_df.fillna(0, inplace=True) # Fill NaNs with 0 if any periods have no trips
    return trip_counts_df

def get_plot(trip_counts_df):
    
    lines = [
        'CR-Fairmount', 'CR-Fitchburg', 'CR-Franklin', 'CR-Greenbush',
        'CR-Worcester', 'CR-Haverhill',
        'CR-Kingston', 'CR-Lowell', 'CR-Middleborough',
        'CR-Needham', 'CR-Newburyport', 'CR-Providence'
    ]
    time_periods = ['AM Peak', 'Early AM', 'Evening', 'Late Evening', 'Midday Base', 
                    'Midday School', 'Night', 'PM Peak', 'Sunrise']
    years = [2019, 2020, 2021, 2022, 2023, 2024]
    colors = sns.color_palette("husl", len(years)) 
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 10), sharey=True)
    axes = axes.flatten()
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    
    for line in lines:
        fig.suptitle(line)
        
        # Plot weekdays
        for i, day in enumerate(weekdays):
            for j, year in enumerate(years):
                try:
                    day_data = trip_counts_df.loc[(line, year, day)]
                    axes[i].plot(time_periods, day_data[time_periods].values.flatten(), 
                                 marker='o', label=year, color=colors[j])
                except KeyError:
                    print(f"No data for {line} in {year} on {day}.")
            
            axes[i].set_title(day)
            axes[i].set_xlabel('Time Period')
            axes[i].set_xticks(range(len(time_periods)))
            axes[i].set_xticklabels(time_periods, rotation=45)
            if i == 0:
                axes[i].set_ylabel('Trip Count')

        # Plot weekend average
        for j, year in enumerate(years):
            try:
                weekend_data = trip_counts_df.loc[(line, year)]
                weekend_data = trip_counts_df[
                    trip_counts_df.index.get_level_values('Day').isin(['Saturday', 'Sunday'])
                ]
                weekend_avg = weekend_data[time_periods].mean().values
                axes[5].plot(time_periods, weekend_avg, marker='o', color=colors[j], label=year)
            except Exception as e:
                print(f"Error processing weekend data for {year}: {e}")
                continue

    # Weekend subplot settings
    axes[5].set_title('Weekend')
    axes[5].set_xlabel('Time Period')
    axes[5].set_xticks(range(len(time_periods)))
    axes[5].set_xticklabels(time_periods, rotation=45)

    # Add legend
    axes[0].legend(title='Year', loc='upper left', bbox_to_anchor=(1.05, 1))

    # Adjust layout
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    
    # Return the figure
    return fig

# trip = get_tripcount_weekday(years)
# print(trip.head())