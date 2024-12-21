import requests
from bs4 import BeautifulSoup
from io import BytesIO
from io import StringIO
import re
import zipfile
import pandas as pd
import os
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)
# import bigquery_cleaned_pipeline as bcp
import matplotlib.pyplot as plt
import matplotlib.cm

table_names = ['trips', 'calendar', 'stop_times', 'routes']

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

db = get_db()

# Function to load and clean data
def load_cleaned_data(base_dir, years, seasons):
    calendar_columns = ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"]
    stop_times_columns = ["trip_id", "arrival_time", "departure_time", "stop_id"]
    trips_columns = ["route_id", "service_id", "trip_id", "direction_id"]
    cleaned_data = {}

    for year in years:
        cleaned_data[year] = {}
        for season in seasons:
            cleaned_data[year][season] = {}
            cleaned_data_temp = {}
            try:
                for table in table_names:
                    filtered_data = db[(db['season'] == season) & (db['year'] == str(year))]
                    if not filtered_data.empty:
                        zip_url = filtered_data.iloc[0]['archive_url']
                        table_data = extract_table_data(zip_url, table, season, year)
                        if table == "calendar":
                            calendar_df = pd.DataFrame(table_data, columns=calendar_columns)
                            calendar_df.dropna(inplace=True)
                            cleaned_data_temp["calendar"] = calendar_df
                        elif table == "stop_times":
                            stop_times_df = pd.DataFrame(table_data, columns=stop_times_columns)
                            stop_times_df.dropna(inplace=True)
                            cleaned_data_temp["stop_times"] = stop_times_df
                        elif table == "trips":
                            trips_df = pd.DataFrame(table_data, columns=trips_columns)
                            trips_df.dropna(inplace=True)
                            cleaned_data_temp["trips"] = trips_df
                        elif table == "routes":
                            routes_df = pd.DataFrame(table_data)
                            commuter_routes = routes_df[routes_df['route_desc'].str.contains("Commuter Rail", na=False)][['route_id', 'route_desc']]
                            cleaned_data_temp["routes"] = commuter_routes
            
                cleaned_data[year][season] = {
                    "calendar": cleaned_data_temp["calendar"],
                    "stop_times": cleaned_data_temp["stop_times"],
                    "trips": cleaned_data_temp["trips"],
                    "routes": cleaned_data_temp["routes"]
                }
            except Exception as e:
                # print(f'error: {e}')
                continue
    
    return cleaned_data
            

    # for year in years:
    #     cleaned_data[year] = {}
    #     for season in seasons:
    #         # Define file paths
    #         calendar_path = os.path.join(base_dir, str(year), f"{season}", "calendar.txt")
    #         stop_times_path = os.path.join(base_dir, str(year), f"{season}", "stop_times.txt")
    #         trips_path = os.path.join(base_dir, str(year), f"{season}", "trips.txt")
    #         routes_path = os.path.join(base_dir, str(year), f"{season}", "routes.txt")

    #         # Load data
    #         calendar_df = pd.read_csv(calendar_path, usecols=calendar_columns)
    #         stop_times_df = pd.read_csv(stop_times_path, usecols=stop_times_columns)
    #         trips_df = pd.read_csv(trips_path, usecols=trips_columns)
    #         routes_df = pd.read_csv(routes_path)

    #         # Clean data
    #         calendar_df.dropna(inplace=True)
    #         stop_times_df.dropna(inplace=True)
    #         trips_df.dropna(inplace=True)

    #         # Filter for commuter rail routes
    #         commuter_routes = routes_df[routes_df['route_desc'].str.contains("Commuter Rail", na=False)][['route_id', 'route_desc']]

    #         # Store cleaned data
    #         cleaned_data[year][season] = {
    #             "calendar": calendar_df,
    #             "stop_times": stop_times_df,
    #             "trips": trips_df,
    #             "routes": commuter_routes
    #         }
    # return cleaned_data

# Function to merge cleaned data
def merge_cleaned_data(cleaned_data):
    merged_data = {}

    for year in cleaned_data:
        merged_data[year] = {}
        for season in cleaned_data[year]:
            try:
                # Extract individual DataFrames
                calendar_df = cleaned_data[year][season]["calendar"]
                trips_df = cleaned_data[year][season]["trips"]
                stop_times_df = cleaned_data[year][season]["stop_times"]
                commuter_routes = cleaned_data[year][season]["routes"]

                # Merge the datasets
                merged_calendar_trips = pd.merge(trips_df, calendar_df, on='service_id')
                merged_full = pd.merge(merged_calendar_trips, stop_times_df, on='trip_id')
                merged_final = pd.merge(merged_full, commuter_routes, on='route_id', how='inner')

                # Store the merged data
                merged_data[year][season] = merged_final
            except Exception as e:
                # print(f'error: {e}')
                continue

    return merged_data

# Function to filter commuter rail data
def filter_commuter_rail_data(merged_data, target_desc='Commuter Rail'):
    for year in merged_data:
        for season in merged_data[year]:
            merged_df = merged_data[year][season]
            # Filter rows based on route description
            filtered_df = merged_df[merged_df['route_desc'] == target_desc]
            merged_data[year][season] = filtered_df
    return merged_data

# Function to calculate trip frequency
def calculate_trip_frequency(merged_data):
    frequency_data = []

    for year in merged_data:
        for season in merged_data[year]:
            season_df = merged_data[year][season]
            unique_trip_count = season_df['trip_id'].nunique()

            frequency_data.append({
                'year': year,
                'season': season,
                'unique_trip_count': unique_trip_count
            })

    frequency_df = pd.DataFrame(frequency_data)
    frequency_df.sort_values(by=['year', 'season'], inplace=True)

    return frequency_df

# Function to calculate average departure times

def calculate_average_departure_times(merged_data):
    """
    Calculate the average departure times in minutes past midnight for each year and season.
    Returns a DataFrame.
    """
    average_departure_times = []

    for year in merged_data:
        for season in merged_data[year]:
            season_df = merged_data[year][season].copy()
            season_df['departure_datetime'] = pd.to_datetime(
                season_df['departure_time'], format='%H:%M:%S', errors='coerce'
            )
            season_df = season_df.dropna(subset=['departure_datetime'])
            season_df['minutes_past_midnight'] = (
                season_df['departure_datetime'].dt.hour * 60 + season_df['departure_datetime'].dt.minute
            )
            avg_minutes = season_df['minutes_past_midnight'].mean()
            average_departure_times.append({
                'year': year,
                'season': season,
                'average_departure_time': avg_minutes
            })

    # Convert the result into a DataFrame
    avg_departure_df = pd.DataFrame(average_departure_times)
    avg_departure_df = avg_departure_df.pivot(index='year', columns='season', values='average_departure_time')
    avg_departure_df.reset_index(inplace=True)
    return avg_departure_df





# --- Data Cleaning ---
def clean_time_data(df):
    """
    Cleans the input DataFrame by ensuring valid time formats for arrival and departure times.
    Rows with invalid times are removed.
    """
    def is_valid_time(time_str):
        try:
            pd.to_datetime(time_str, format='%H:%M:%S')
            return True
        except ValueError:
            return False

    valid_arrival = df['arrival_time'].apply(is_valid_time)
    valid_departure = df['departure_time'].apply(is_valid_time)
    return df[valid_arrival & valid_departure]


# --- Trip Duration Calculation ---
def calculate_trip_durations(df):
    """
    Calculates trip durations by grouping the data by route and trip IDs.
    Returns a DataFrame with trip IDs, route IDs, and trip durations.
    """
    df = clean_time_data(df)
    df['arrival_time'] = pd.to_datetime(df['arrival_time'], format='%H:%M:%S')
    df['departure_time'] = pd.to_datetime(df['departure_time'], format='%H:%M:%S')

    grouped = df.groupby(['route_id', 'trip_id']).apply(
        lambda x: x['departure_time'].iloc[-1] - x['arrival_time'].iloc[0]
    ).reset_index(name='trip_duration')
    return grouped


def calculate_average_trip_duration_per_route(trip_durations):
    """
    Aggregates trip durations for each route and calculates the average trip duration.
    Returns a DataFrame with route IDs and their average trip durations.
    """
    average_durations = trip_durations.groupby('route_id')['trip_duration'].mean().reset_index()
    average_durations.rename(columns={'trip_duration': 'average_trip_duration'}, inplace=True)
    return average_durations


# --- Data Aggregation ---
def aggregate_route_data(merged_data):
    """
    Processes merged data to calculate average trip durations for each route across all seasons and years.
    Returns a dictionary of route DataFrames for each route ID.
    """
    route_data = {}

    for year, seasons in merged_data.items():
        for season, df in seasons.items():
            trip_durations = calculate_trip_durations(df)
            average_trip_durations = calculate_average_trip_duration_per_route(trip_durations)

            season_year = f"{season}{year}"
            
            for _, row in average_trip_durations.iterrows():
                route_id = row['route_id']
                avg_duration = row['average_trip_duration']

                if route_id not in route_data:
                    route_data[route_id] = []

                route_data[route_id].append({
                    'season-year': season_year,
                    'average_trip_duration': avg_duration
                })

    route_dfs = {
        route_id: pd.DataFrame(data).sort_values(by='season-year')
        for route_id, data in route_data.items()
    }
    return route_dfs


def combine_route_data(route_dfs):
    """
    Combines route-specific data into a single DataFrame for easier analysis and visualization.
    Converts durations to minutes and organizes data by route and season-year.
    """
    combined_route_data = []

    for route_id, data in route_dfs.items():
        data['route_name'] = route_id
        data['average_trip_duration_minutes'] = data['average_trip_duration'].dt.total_seconds() / 60.0
        combined_route_data.append(data[['route_name', 'season-year', 'average_trip_duration_minutes']])

    combined_df = pd.concat(combined_route_data, ignore_index=True)
    combined_df.sort_values(by=['route_name', 'season-year'], inplace=True)
    return combined_df


# --- Main Function ---






# Main function
def main():
    base_dir = "./datasets"
    years = [2019, 2020, 2021, 2022, 2023, 2024]
    seasons = ["Spring", "Fall", "Summer", "Winter"]

    # Step 1: Load and clean data
    cleaned_data = load_cleaned_data(base_dir, years, seasons)

    # Step 2: Merge data
    merged_data = merge_cleaned_data(cleaned_data)

    # Step 3: Filter commuter rail data
    filtered_merged_data = filter_commuter_rail_data(merged_data)

    # Step 4: Analyze trip frequency
    frequency_df = calculate_trip_frequency(filtered_merged_data)
    # print("Train Frequency Analysis (Unique Trip Counts) by Season-Year:")
    # print(frequency_df)

    # Step 5: Analyze average departure times
    average_departure_times = calculate_average_departure_times(filtered_merged_data)
    # print("\nAverage Departure Times (Minutes Past Midnight) by Year and Season:")
    # print(average_departure_times)

    route_dfs = aggregate_route_data(merged_data)

    combined_df = combine_route_data(route_dfs)

    return combined_df
    print(combined_df)

def plot_all_routes_chronological(df):
    # Split the `season-year` column into `season` and `year`
    df['season'] = df['season-year'].str[:-4]  
    df['year'] = df['season-year'].str[-4:]  

    # Define a custom order for the seasons
    season_order = ["Fall", "Winter", "Spring", "Summer"]
    
    # Create a custom sorting key: first by year, then by the season order
    df['season_rank'] = df['season'].map({season: i for i, season in enumerate(season_order)})
    df['sort_key'] = df['year'] + df['season_rank'].astype(str)  
    df = df.sort_values(by=['sort_key']) 

    # Combine `season` and `year` into properly formatted labels for the x-axis
    df['season-year-clean'] = df['season'] + df['year'] 

    # Create the figure
    fig, ax = plt.subplots(figsize=(15, 8))
    
    # Generate a unique color for each route using a colormap
    unique_routes = df['route_name'].unique()
    num_routes = len(unique_routes)
    colors = matplotlib.cm.get_cmap('tab20', num_routes)  # Using the 'tab20' colormap for distinct colors
    
    # Plot a line for each route
    for i, route_id in enumerate(unique_routes):
        route_df = df[df['route_name'] == route_id]
        ax.plot(route_df['season-year-clean'], 
                route_df['average_trip_duration_minutes'], 
                marker='o', 
                label=route_id, 
                color=colors(i))  # Assign a unique color
    
    # Formatting the plot
    ax.set_title("Average Trip Duration for All Routes (Chronological)", fontsize=18)
    ax.set_xlabel("Season-Year", fontsize=16)
    ax.set_ylabel("Average Trip Duration (minutes)", fontsize=16)
    ax.tick_params(axis='x', rotation=45, labelsize=12)
    ax.tick_params(axis='y', labelsize=14)
    
    # Move the legend outside the plot or to a better position
    ax.legend(
        title="Routes", 
        fontsize=10, 
        title_fontsize=12, 
        loc='upper left', 
        bbox_to_anchor=(1, 1) 
    )
    ax.grid()
    
    # Ensure layout doesn't cut off labels
    fig.tight_layout()
    
    # Return the figure object
    return fig

# Execute the main function
if __name__ == "__main__":
    main()