# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# Hi!
# This Notebook is used to analyze the question: What's the net number of trains operating per line.

# Lets start by installing the required libraries running the following sentence:

# pip install requests pandas

# Now, we need to import some essential Libaries! Please run the following cell:

import os
import requests
import zipfile
from io import BytesIO
from datetime import datetime
import csv
import pandas as pd

# We are ready for the environment for this question now. Let's get the basic datasets for this question. By running the following cell, it will automatically download the datasets for "trips" datasets for 2019-2024.
# Note: If new datasets are uploaded to the Base url, you just need to change the numbers in YEARS = range(2019, 2025). It will download the newest datasets for you.

# +
# Download dataset from MBTA archived feeds.
BASE_URL = "https://cdn.mbta.com/archive/archived_feeds.txt"
DOWNLOAD_DIR = "mbta_datasets"
YEARS = range(2019, 2025)  # Range of years to download
SEASONS = ["winter", "spring", "summer", "fall"]  # MBTA seasons
TRIPS_FILENAME = "trips.txt"  # Name of the dataset file in the zip

# Create download directory if it doesn't exist
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def fetch_data_urls(base_url):
    """
    Fetches the dataset URLs from the archive file.
    Returns a dictionary of the newest URLs for each season.
    """
    response = requests.get(base_url)
    response.raise_for_status()
    lines = response.text.splitlines()

    # Parse the file as CSV since it contains structured metadata
    reader = csv.reader(lines)
    header = next(reader)  # Skip header
    url_index = header.index("archive_url")  # Find the column with the URLs
    date_index = header.index("feed_start_date")  # Find the column with the start date

    dataset_records = []
    for row in reader:
        if len(row) > url_index and row[url_index].startswith("http"):
            dataset_records.append({
                "url": row[url_index],
                "date": row[date_index],
                "season": get_season_from_date(row[date_index])
            })

    # Filter datasets for the desired years and seasons
    newest_urls = {}
    for year in YEARS:
        for season in SEASONS:
            season_records = [
                record for record in dataset_records 
                if record["season"] == season and record["date"].startswith(str(year))
            ]
            if season_records:
                # Sort by date to get the newest file
                season_records.sort(key=lambda x: x["date"], reverse=True)
                newest_urls[f"{year}_{season}"] = season_records[0]["url"]
    
    return newest_urls

def get_season_from_date(date_str):
    """
    Determines the season based on the date.
    """
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    if date_obj.month in [12, 1, 2]:
        return "winter"
    elif date_obj.month in [3, 4, 5]:
        return "spring"
    elif date_obj.month in [6, 7, 8]:
        return "summer"
    else:
        return "fall"

def download_and_extract(url, season_year):
    """
    Downloads and extracts the trips dataset from a zip file URL and renames it.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            # List all files in the archive
            file_list = z.namelist()
            # Find the file named exactly "trips.txt"
            if TRIPS_FILENAME in file_list:
                print(f"Extracting {TRIPS_FILENAME} from {url}...")
                # Define the renamed file path
                renamed_file_path = os.path.join(DOWNLOAD_DIR, f"{season_year}_trips.txt")
                # Extract and rename the file
                with z.open(TRIPS_FILENAME) as original_file, open(renamed_file_path, "wb") as renamed_file:
                    renamed_file.write(original_file.read())
                print(f"Saved as {renamed_file_path}.")
            else:
                print(f"'{TRIPS_FILENAME}' not found in {url}")
    except Exception as e:
        print(f"Failed to download or extract from {url}: {e}")

# def main():
print(f"Fetching dataset URLs from {BASE_URL}...")
newest_data_urls = fetch_data_urls(BASE_URL)

print(f"Found {len(newest_data_urls)} datasets (newest for each season).")
for season_year, url in newest_data_urls.items():
    # Download and extract each dataset with a renamed file
    download_and_extract(url, season_year)

# if __name__ == "__main__":
#     main()
# -

# Finally we have everthing ready for our data analysis! Now we can start the data cleaning process for these datasets. Let's filter out the commuter rail data and doing some data cleaning process.
# First, running the following cell will merge all downloaded datasets into a CSV file that only contains Commuter rail data with years and seasons.
# We will use this merged CSV file to make further analysis.

# +
# Directory containing the downloaded files
RAW_DATA_DIR = "mbta_datasets"
MERGED_FILE = "merged_trips.csv"
FILTERED_FILE = "filtered_commuter_rail_trips.csv"

def merge_all_datasets(raw_dir, merged_file):
    """
    Merges all trips datasets into a single CSV file.

    """
    files = [f for f in os.listdir(raw_dir) if f.endswith("_trips.txt")]

    if not files:
        print("No files found in the directory.")
        return pd.DataFrame()

    merged_df = pd.DataFrame()

    for file in files:
        file_path = os.path.join(raw_dir, file)
        print(f"Reading {file_path}...")
        df = pd.read_csv(file_path, low_memory=False)
        # Add year and season based on the file name
        parts = file.split("_")
        year = parts[0]
        season = parts[1].replace("trips.txt", "").strip()
        df["year"] = year
        df["season"] = season
        merged_df = pd.concat([merged_df, df], ignore_index=True)

    # Save the merged dataset
    merged_df.to_csv(merged_file, index=False)
    print(f"Merged dataset saved to {merged_file}.")
    return merged_df

def filter_commuter_rail_data(merged_df, filtered_file):
    """
    Filters rows where `route_id` contains 'CR' and saves the result.

    """
    if "route_id" not in merged_df.columns:
        print("'route_id' column not found in the merged dataset.")
        return pd.DataFrame()

    # Filter for rows where route_id contains "CR"
    filtered_df = merged_df[merged_df["route_id"].str.contains("CR", na=False, case=False)]

    # Save the filtered dataset
    filtered_df.to_csv(filtered_file, index=False)
    print(f"Filtered dataset saved to {filtered_file}.")
    return filtered_df

print("Merging all datasets...")
merged_df = merge_all_datasets(RAW_DATA_DIR, MERGED_FILE)

if not merged_df.empty:
    print("Filtering commuter rail data...")
    filter_commuter_rail_data(merged_df, FILTERED_FILE)
else:
    print("No data to filter.")


# -

# Now the following cell will process the data cleanning process and create a new cleaned CSV. This is the final step for us to make further analysis!

# +
# Path to the filtered commuter rail dataset
FILTERED_FILE = "filtered_commuter_rail_trips.csv"
CLEANED_FILE = "cleaned_commuter_rail_trips.csv"

def clean_dataset(file_path, output_file):
    """
    Cleans the filtered commuter rail dataset with multiple cleaning steps.
    
    Args:
        file_path (str): Path to the filtered dataset.
        output_file (str): Path to save the cleaned dataset.
    
    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    try:
        # Load the dataset
        print(f"Loading dataset from {file_path}...")
        df = pd.read_csv(file_path, low_memory=False)

        print(f"Initial dataset shape: {df.shape}")

        # 1. Remove duplicates
        df = df.drop_duplicates()
        print(f"After removing duplicates: {df.shape}")

        # 2. Check for rows with missing values
        missing_values = df.isnull().sum()
        print("Missing values by column before handling:")
        print(missing_values)

        # Drop rows with missing values in essential columns
        essential_columns = ['route_id', 'trip_id', 'service_id']  # Adjust based on dataset
        df = df.dropna(subset=essential_columns)
        print(f"After dropping rows with missing essential values: {df.shape}")

        # Fill missing values in non-essential columns
        df = df.fillna({
            'direction_id': 0,  # Assuming 0 is default
            'shape_id': 'UNKNOWN',  # Placeholder for missing shape_id
        })

        # 3. Validate and standardize string columns
        string_columns = ['route_id', 'trip_id', 'service_id', 'shape_id']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].str.strip().str.upper()  # Strip whitespace, make uppercase

        # 4. Validate numeric columns
        if 'direction_id' in df.columns:
            df['direction_id'] = pd.to_numeric(df['direction_id'], errors='coerce').fillna(0).astype(int)

        # Debugging: Check for invalid values
        invalid_routes = df[~df['route_id'].str.startswith('CR-')]
        print(f"Rows with invalid 'route_id': {invalid_routes.shape[0]}")

        # 5. Drop invalid rows (optional, depending on the analysis context)
        df = df[df['route_id'].str.startswith('CR-', na=False)]
        print(f"After filtering invalid 'route_id': {df.shape}")

        # 6. Add derived columns
        # Example: Add a column to indicate weekday vs. weekend service
        if 'service_id' in df.columns:
            df['is_weekend'] = df['service_id'].str.contains('SAT|SUN', case=False, na=False)

        # 7. Remove unnecessary columns
        unnecessary_columns = ['extra_column1', 'extra_column2']  # Replace with actual column names
        df = df.drop(columns=[col for col in unnecessary_columns if col in df.columns], errors='ignore')

        print(f"Final dataset shape: {df.shape}")

        # Save the cleaned dataset
        df.to_csv(output_file, index=False)
        print(f"Cleaned dataset saved to {output_file}.")
        return df

    except Exception as e:
        print(f"Error during data cleaning: {e}")
        return None

def main():
    print("Starting data cleaning process...")
    cleaned_df = clean_dataset(FILTERED_FILE, CLEANED_FILE)
    # if cleaned_df is not None:
    #     print("Data cleaning process completed successfully.")
    # else:
    #     print("Data cleaning process failed.")
    return cleaned_df


cleaned_df = main()

import matplotlib.pyplot as plt

# Load the cleaned dataset
file_path = "cleaned_commuter_rail_trips.csv"  # Adjust path if necessary
# df = pd.read_csv(file_path)

# Group by route_id (line) and count unique trip_id (trains)
net_trains_per_line = cleaned_df.groupby("route_id")["trip_id"].nunique().reset_index()
net_trains_per_line.columns = ["route_id", "net_trains"]

# Sort by the number of trains for better visualization
net_trains_per_line = net_trains_per_line.sort_values(by="net_trains", ascending=False)

# Visualization
fig_net_trains, ax = plt.subplots(figsize=(12, 8))

# Plot the bar chart
bars = ax.bar(net_trains_per_line["route_id"], net_trains_per_line["net_trains"], color='skyblue')

# Add numbers on the bars
for bar in bars:
    yval = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, yval + 2, int(yval), ha="center", va="bottom")

# Chart aesthetics
ax.set_title("Net Number of Trains Operating per Line", fontsize=16)
ax.set_xlabel("Route ID (Line)", fontsize=12)
ax.set_ylabel("Net Trains", fontsize=12)
ax.set_xticks(range(len(net_trains_per_line["route_id"])))
ax.set_xticklabels(net_trains_per_line["route_id"], rotation=45, ha="right")

# Adjust layout
fig_net_trains.tight_layout()

# # Save the result to a CSV file
# output_file = "net_trains_per_line.csv"
# net_trains_per_line.to_csv(output_file, index=False)
# print(f"Net number of trains per line saved to {output_file}.")

# # -

# # Now let's see some interesting findings. Running the following cell will generate a year-over-year growth graph to show the change of numbers over year 2019 to year 2024!

# # +
# # Load the cleaned dataset
# file_path = "cleaned_commuter_rail_trips.csv"  # Adjust path if necessary
# df = pd.read_csv(file_path)

# # Group by year and calculate the total net number of unique trains
# yearly_trains = df.groupby("year")["trip_id"].nunique().reset_index()
# yearly_trains.columns = ["year", "net_trains"]

# # Ensure the year is sorted
# yearly_trains["year"] = yearly_trains["year"].astype(int)
# yearly_trains = yearly_trains.sort_values(by="year")

# # Calculate year-over-year growth
# yearly_trains["yoy_growth"] = yearly_trains["net_trains"].pct_change() * 100

# # Visualization
# plt.figure(figsize=(12, 8))
# bars = plt.bar(yearly_trains["year"].astype(str), yearly_trains["yoy_growth"].fillna(0), color='orange')

# # Add numbers on the bars
# for bar in bars:
#     yval = bar.get_height()
#     plt.text(bar.get_x() + bar.get_width() / 2, yval + 2 if yval > 0 else yval - 5, f"{yval:.1f}%", 
#              ha="center", va="bottom" if yval > 0 else "top", fontsize=10)

# # Chart aesthetics
# plt.title("Year-over-Year Growth of Net Number of Trains", fontsize=16)
# plt.xlabel("Year", fontsize=12)
# plt.ylabel("YoY Growth (%)", fontsize=12)
# plt.axhline(0, color="gray", linestyle="--", linewidth=0.8)  # Add a horizontal line at 0% growth
# plt.xticks(rotation=45, ha="right")
# plt.tight_layout()

# # Show the plot
# plt.show()

# # -

# # We can also do seasonal analysis for this question! Let's see what changes are made by different seasons!

# # +
# # Load the cleaned dataset
# file_path = "cleaned_commuter_rail_trips.csv"  # Adjust path if necessary
# df = pd.read_csv(file_path)

# # Define the seasons
# seasons = ["SPRING", "SUMMER", "FALL", "WINTER"]

# # Function to create a seasonal plot
# def plot_seasonal_data(df, season):
#     # Filter data for the given season
#     seasonal_data = df[df["season"].str.upper() == season]

#     # Group by route_id (line) and count unique trip_id (trains)
#     seasonal_trains = seasonal_data.groupby("route_id")["trip_id"].nunique().reset_index()
#     seasonal_trains.columns = ["route_id", "net_trains"]
#     seasonal_trains = seasonal_trains.sort_values(by="net_trains", ascending=False)

#     # Plot the data
#     plt.figure(figsize=(12, 8))
#     bars = plt.bar(seasonal_trains["route_id"], seasonal_trains["net_trains"], color="skyblue")

#     # Add numbers on the bars
#     for bar in bars:
#         yval = bar.get_height()
#         plt.text(bar.get_x() + bar.get_width() / 2, yval + 2, int(yval), ha="center", va="bottom")

#     # Chart aesthetics
#     plt.title(f"Net Number of Trains per Line - {season.capitalize()}", fontsize=16)
#     plt.xlabel("Route ID (Line)", fontsize=12)
#     plt.ylabel("Net Trains", fontsize=12)
#     plt.xticks(rotation=45, ha="right")
#     plt.tight_layout()
#     plt.show()

# # Generate plots for each season
# for season in seasons:
#     plot_seasonal_data(df, season)

# # +
# Load the cleaned dataset
file_path = "cleaned_commuter_rail_trips.csv"  # Adjust path if necessary
df = pd.read_csv(file_path)

# Define the seasons
seasons = ["Spring", "Summer", "Fall", "Winter"]

# # Function to create a seasonal bar plot
def plot_seasonal_analysis(data, season):
    # Filter data for the given season
    seasonal_data = data[data["season"].str.upper() == season.upper()]

    # Group by route_id and year to count unique trip_id (trains)
    seasonal_trains = (
        seasonal_data.groupby(["route_id", "year"])["trip_id"]
        .nunique()
        .reset_index()
        .rename(columns={"trip_id": "net_trains"})
    )

    # Pivot for visualization
    seasonal_pivot = seasonal_trains.pivot(index="route_id", columns="year", values="net_trains").fillna(0)

    # Plot
    seasonal_pivot.plot(kind="bar", figsize=(15, 8), colormap="viridis", edgecolor="black")

    # Add title and labels
    plt.title(f"Seasonal Analysis for {season.capitalize()} (2019-2024)", fontsize=16)
    plt.xlabel("Route ID (Lines)", fontsize=12)
    plt.ylabel("Number of Trains", fontsize=12)
    plt.legend(title="Year", fontsize=10)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()

# # Generate plots for each season
# for season in seasons:
#     plot_seasonal_analysis(df, season)
# -


