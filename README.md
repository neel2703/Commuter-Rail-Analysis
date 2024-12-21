## Overview 
The project aims to build a comprehensive data repository and analysis framework for MBTA (Massachusetts Bay Transportation Authority) schedules, focusing on both current and historical data. This initiative will support policy-making and decarbonization strategies by providing insights into transit schedules, timing, and operational changes over time. 

The project begins by aggregating data from the MBTA API and creating a database. The database will be uploaded to a cloud service for accessibility and scalability. Additionally, scripts will be developed to automatically fetch the most current data and retrieve archived data for analysis.

We will be focusing on answering the questions listed below:
- How have schedules shifted over time?
- What is the net number of trains operating per line?
- How do travel times vary across different schedules?
- How do station changes impact overall transit schedules?
- A breakdown of trains by time of day (early morning, peak morning, midday, afternoon peak, evening, late night)
- Identify/analyze the number of express trains per line
- Do we see incidents of a station changing fare zones?
- If cost info is included in the active GTFS feed & archives, can we compare the changes in fare cost over time?

We also plan to develop a streamlined pipeline to extract raw data from the MBTA archives and load it into BigQuery for seamless storage and analysis. Additionally, we will create a separate pipeline to upload the answers to each question directly into BigQuery, ensuring efficient organization and accessibility of insights.

## Client:
A Better City represents a multi-sector group of nearly 130 business leaders united around a common goal: to enhance the Greater Boston region’s economic health, competitiveness, equitable growth, sustainability, and quality of life for all communities. By amplifying the voice of the business community through collaboration and consensus-building, A Better City develops solutions and influences policy in three critical areas: 1) transportation and infrastructure, 2) land use and development, and 3) energy and the environment. A Better City is committed to building an equitable and inclusive future for the region that benefits and uplifts residents, workers, and businesses in Greater Boston. 

## Dataset Description
The dataset originated from the MBTA archives and is scraped for the latest version for each season and year. The scraped URL contains 31 tables containing information on routes, lines, schedules, trips, fares, etc. which helps analyze each project question. This raw dataset is also uploaded to BigQuery for feasible preprocessing and analysis.

Packages Used
- Os: Provides a way to interact with the operating system. Commonly used for file and directory operations, such as creating, reading, deleting files or navigating file paths.
- Requests: A Python library for making HTTP requests to interact with web services and APIs. Often used to download data from the web (e.g., JSON, files, or HTML pages).
- Zipfile: A library that allows working with ZIP archives (compressed files). Used to create, read, write, and extract files from .zip archives.
- BytesIO (from io): Provides an in-memory binary stream. Useful when dealing with binary data (e.g., reading or writing binary files) without needing to write to disk. Often used in conjunction with requests when working with files retrieved from the web, such as ZIP files.
- Datetime: Provides classes for manipulating dates and times. Useful for tasks such as formatting, parsing, and calculating differences between dates or times.
- Csv: A library to work with CSV (Comma-Separated Values) files. Allows reading and writing tabular data stored in text files in a structured way.
- Pandas: A powerful data manipulation and analysis library. Provides tools for reading and writing data in various formats (e.g., CSV, Excel, SQL), and working with data frames for analysis, manipulation, and visualization.
- matplotlib.pyplot: Used for creating visualizations like line plots, bar graphs, and scatter plots.	
- pandas: Used for data manipulation and analysis, such as reading, merging, and filtering datasets.				
- numpy: Used for numerical operations, including calculations and handling arrays efficiently.				
- matplotlib.cm: Provides colormap functionalities for customizing plot aesthetics.
  
# Questions

## Q1)A breakdown of trains by time of day (early morning, peak morning, midday, afternoon peak, evening, late night)

- 1.1 Data source
	- Open https://cdn.mbta.com/archive/archived_feeds.txt , and click on the latest updated zip file for each season. Each zip file will contain multiple .txt files for each table in the database. For this question, we focused on the following datasets:
		calendar.txt
		trips.txt
		stop_times.txt
		routes.txt
- 1.2 Data cleaning steps
	- Drop all NULL and Duplicate values.

- 1.3 Preliminary analysis
	- Define the time periods of the day in the function time_of_day
		Assign the time period to each trip by applying the time_of_day function on each trip
	- Extract service_id from calendar data for each season from 2019 to 2024 using the get_weekday_services function. 
		The filter_weekday_trips function will iterate through all the data and merge the stop_times and trips. It will filter the working schedule using service_id and return a dataframe containing trip count, time period, route id, year, and seasons. The plotted graph is the trip counts by time period for each commuter rail. The x-axis represents the time period and y axis represents the trip counts. 
	- Merged the trip and calendar data by stop times, trips, and calendar. 
		The get_mergeddata function will iterate through each season in each year and merge stop times and trips by trip id. The returned data frame contains information of stop times, trips, and calendar. 
	- Get the merged calendar from 19 to 20. 
		Go through each commuter rail line and analyze the train operation on weekdays and weekends. The output data frame contains the number of trips by time period for every weekday, classified by commuter rail line.

- 1.4 Analysis
	The final dataframe contains information of trip number for each commuter rail line, separated by year, time of day, and calendar. One of the limitations of this problem might be due to the missing data from the datasets. There could also be incomplete records, for instance, trips operating outside the regular service hour and were not recorded. This might lead to inaccurate presentation of the change in schedule.
Another limitation is the lack of information on ridership demand. Even though the number of trips reflects the service availability, it does not account for passenger utilization. A high number of scheduled trips does not necessarily indicate high demand. To address this, further analysis could use ridership data, which would have a more comprehensive understanding of the schedule changes.


## Q2)Identify/analyze the number of express trains per line
- 2.1 Data Source
	trips.txt
	stop_times.txt
	stops.txt
- 2.2 Data Cleaning
	Dropped all NULL data and duplicated data. 
	Count the number of stops for each unique trip and merge the trip id and the route id. 
	For each route, we count for the max number of stops. 
		If the number of stops for a trip is less than the maximum number of stops for that route, we assume that this trip has skipped stops, and therefore it is an express train. 
	Visualization: The output graph shows the number of express trips on each commuter rail line from 2021 to 2024. The y axis represents the trip counts and the x axis represents the years. 

## Q3)Do we see incidents of a station changing fare zones?
- 3.1 Data source
 	Open https://cdn.mbta.com/archive/archived_feeds.txt , and click on the latest updated zip file for each season. Each zip file will contain multiple .txt files for each table in the database. For this question, we focused on the following datasets:
	stop.txt
- 3.2 Data cleaning steps
	Drop all NULL and Duplicate values.

- 3.3 Data Preparation
	The data for all the seasons (Spring, Summer, Fall, Winter) was combined into one dataframe for each year. Along with this merging, the filtering of the data to only contain Commuter Rail zones was also done. The datasets were also filtered to only contain data about stations and not other types of locations by matching the location_type column to 1 (location_type == 1) since that value corresponds to stations.
	All of these datasets for each year were then merged into a single dataset again and the changes in stations over those years were then analyzed.

- 3.4 Analysis
	The filtered dataset includes a bunch of unique stations. Some of them are mentioned below:
		Foxboro
		Lynn
  		Oak Grove
		Pawtucket
		Central Falls
		Quincy Center
		River Works 
	The final dataframe containing the analyzed data can be accessed from the final_table() function in the fare_zone_change.py script.


## Q4) If cost info is included in the active GTFS feed & archives, can we compare the changes in fare cost over time?

- 4.1 Data Sources: 
	- Open https://cdn.mbta.com/archive/archived_feeds.txt, and click on the latest updated zip file for each season. Each zip file will contain multiple .txt files for each table in the database. For this question, we focused on the following datasets:
		Fare_products.txt
		fare_leg_rules.txt
		routes.txt
	- Steps to save and access the datasets: 
		To ensure efficient access and analysis, the datasets should be saved in a structured directory hierarchy as outlined below:
			Base Directory: Create a main folder named datasets.
			Yearly Subdirectories: Inside the datasets folder, create subdirectories for each year, labeled as 2019, 2020, 2021, 2022, 2023, and 2024.
			Seasonal Subfolders: Within each yearly directory, organize datasets into folders named Fall, Spring, Summer, and Winter to represent the respective seasons.
			Dataset Placement: Place the relevant datasets into their corresponding season folders under the appropriate year directory.
	- The provided code efficiently loads datasets (e.g., Fare_products.txt, fare_leg_rules.txt, and routes.txt) for specified years and seasons by iterating through the organized directory structure.
	```
	base_dir = "./datasets"
	years = [2019, 2020, 2021, 2022, 2023, 2024]
	seasons = ["Spring", "Fall", "Summer", "Winter"]
 	```

	- Load data code:
	`fare_df = pd.read_csv(fares_path, usecols=calendar_column)` 


- 4.2 Data cleaning: 
	1. Filter out commuter rail data in the routes table
		routes_data = routes_data[routes_data['route_desc'] == 'Commuter Rail']
	2. Drop all the null value columns which are not required
		routes_data = routes_data.dropna(axis=1, how='any')
	3. Combine the fares data of all the years
		combined_fares_df = pd.concat(fares_data.values(), ignore_index=True)
	4. Combine the fare_leg_rules data of all the years
		combined_fares_rules_df = pd.concat(fares_rules_data.values(), ignore_index=True)
	5. Join the fares data and fare_leg_rules data in a single data frame
		combined_fare_result = pd.merge(combined_fares_df, combined_fares_rules_df, on='fare_product_id', how='inner')
	6. Join the resulting table with the routes table into the final data frame
		combined_fare_result = pd.merge(combined_fare_result, routes_data, on='network_id', how='left')
	7. Filter commuter rail and cape flyer data
		combined_fare_result_CR = combined_fare_result[combined_fare_result['network_id'] .isin(['commuter_rail', 'cape_flyer'])]

- 4.3 Analysis - Calculating Average Trip Duration by Route:
	1. Data Context:The dataset includes 14 unique routes, such as:
		"CR-Fairmount"
		"CapeFlyer"
	The fare zone for the north station/south station is area_commuter_rail_zone_1a
	Each route has a final destination station which is present in one of the fare_zones
		'line-Fitchburg': 'area_commuter_rail_zone_8'
		'line-Franklin': 'area_commuter_rail_zone_6'


	2. Objective:
		Calculating the fare to travel from Boston to the last station of each route can be done by mapping the area ID of both arrival and departure stations.

	3. Prepare a Simplified DataFrame: Create a new DataFrame (result_table) with the following columns:
		route_name: Contains the 14 unique routes (e.g., CR-Fairmount, CapeFlyer).
		Amount: Fares to travel from Boston to the last station of that route
		This DataFrame can accessed using the code:
		```
		print(result_table) Under the “main” function
		```


## 5/7. How have schedules shifted over time? And how do travel times vary across different schedules?

- 5.1 Data Sources: 
	- Open https://cdn.mbta.com/archive/archived_feeds.txt , and click on the latest updated zip file for each season. Each zip file will contain multiple .txt files for each table in the database. For this question, we focused on the following datasets:
		calendar.txt
		trips.txt
		stop_times.txt
		routes.txt
	- Steps to save and access the datasets: 
	- To ensure efficient access and analysis, the datasets should be saved in a structured directory hierarchy as outlined below:
		Base Directory: Create a main folder named datasets.
		Yearly Subdirectories: Inside the datasets folder, create subdirectories for each year, labeled as 2019, 2020, 2021, 2022, 2023, and 2024.
		Seasonal Subfolders: Within each yearly directory, organize datasets into folders named Fall, Spring, Summer, and Winter to represent the respective seasons.
		Dataset Placement: Place the relevant datasets into their corresponding season folders under the appropriate year directory.
	- The provided code efficiently loads datasets (e.g., calendar.txt, stop_times.txt, trips.txt, and routes.txt) for specified years and seasons by iterating through the organized directory structure.
		```
		base_dir = "./datasets"
		years = [2019, 2020, 2021, 2022, 2023, 2024]
		seasons = ["Spring", "Fall", "Summer", "Winter"]
  		```

	- Load data code:
	```
	calendar_df = pd.read_csv(calendar_path, usecols=calendar_column)
 	```


- 5.2 Data cleaning: 
	1. Drop NULL values from the calendar dataset: 
		calendar_df.dropna(inplace=True)
	2. Store cleaned datasets in a nested dictionary:
		cleaned_data[year][season] = calendar_df
	Follow the above steps for the other datasets as well (trips, stop_times, routes)
	3. Merge the trips and calendar datasets using “service_id”:
		merged_calendar_trips = pd.merge(trips_df, calendar_df, on='service_id')
	4. Merge the result with the stop_times dataset using “trip_id”:
		merged_full = pd.merge(merged_calendar_trips, stop_times_df, on='trip_id')
	5. Merge the result with the routes dataset using “route_id”:
		merged_final = pd.merge(merged_full, commuter_routes, on='route_id', how='inner')
	6. Filter the merged dataset to retain only "Commuter Rail" data:
		filtered_df = merged_df[merged_df['route_desc'] == 'Commuter Rail']
		merged_data[year][season] = filtered_df
	7. View the dataset for 2020-Spring:
		filtered_merged_data[2020]["Spring"]

- 5.3 Analysis - Calculating Average Trip Duration by Route:
	1. Data Context: The dataset includes 14 unique routes, such as:
		"CR-Fairmount"
		"CapeFlyer"
	Each route consists of multiple trips, identified by a unique trip_id. For example:
		"CR-Saturday-Fall-19-1753"
		"CR-Weekday-StormB-19-7500C0"

	2. Objective:
		Calculate the trip duration for each trip using arrival_time and departure_time from the merged_data DataFrame.
		Compute the average trip duration for all trips within a route to get the average duration for each route.
		Repeat this process for all 14 routes.

	3. Prepare a Simplified DataFrame:
		Create a new DataFrame (combined_df) with the following columns:
			route_name: Contains the 14 unique routes (e.g., CR-Fairmount, CapeFlyer).
			season-year: Represents the season and year (e.g., Fall2019, Spring2020).
			average_trip_duration_minutes: Contains the computed average trip duration for each route and season-year.
		This DataFrame can accessed using the code:
			```
			print(combined_df) Under the “combine_route_data” function
			```

- 5.4 Visualization
	To analyze the average trip durations across seasons and years, a line graph is generated using the combined_df DataFrame:
		X-Axis: Represents the season_year values (e.g., "Fall2019", "Spring2020").
		Y-Axis: Represents the average_duration for each route.
		Lines: Each of the 14 routes is represented as a distinct line, differentiated by unique colors.
	The visualization is implemented using the function:
		```
		def plot_all_routes_chronological(df):
  		```


## 6.  What is the net number of trains operating per line?


- 6.1 Data Sources: 
	- Open https://cdn.mbta.com/archive/archived_feeds.txt, and click on the latest updated zip file for each season. Each zip file will contain multiple .txt files for each table in the database. For this question, we focused on the following datasets:
		Trips.txt


- 6.2 Data process and cleaning: def fetch_data_urls(base_url)
	- This Python script processes and cleans commuter rail trip data from text files. The process starts with the merge_all_datasets function. This function scans a directory (RAW_DATA_DIR) for files ending with _trips.txt. It reads each file into a pandas DataFrame. The script extracts metadata, such as year and season, from the filenames. It adds this metadata to the DataFrame as new columns. All the individual datasets are then combined into one DataFrame (merged_df). The merged data is saved as a CSV file named MERGED_FILE.
	- The filter_commuter_rail_data function performs the next step. It filters the merged data to keep only rows where the route_id column contains "CR". These rows represent commuter rail routes. The filtered data is saved as another CSV file, FILTERED_FILE.
	- The main function coordinates the workflow. It first merges all datasets. If the merged dataset is not empty, it filters the data. This ensures that only relevant commuter rail information is processed. Since 2019 is the first year, it contributes 0% of the data. This step-by-step approach ensures clean and organized data for further analysis.

	- Steps to save and access datasets: def clean_dataset(file_path, output_file)
		This code defines a data cleaning function called clean_dataset. It starts by removing duplicate rows from the DataFrame to ensure data integrity. It then checks for missing values and prints a summary for reference. Rows with missing values in essential columns, such as route_id, trip_id, and service_id, are removed. For non-essential columns, such as direction_id and shape_id, missing values are filled with default values.
		The function standardizes string columns by trimming whitespace and converting text to uppercase. It also validates numeric columns, like direction_id, by converting them to integers and filling invalid values with defaults. Rows with invalid data, such as those where route_id does not start with "CR," are filtered out.
		The function adds a derived column, is_weekend, to indicate if a trip operates on weekends. It also removes unnecessary columns to keep the dataset focused. Finally, it saves the cleaned dataset to a CSV file (output_file). Each step provides clear feedback on the changes to the data. Since 2019 is the starting year, it contributes 0% of the data. This ensures a clean and organized dataset ready for analysis.

- 6.3 Analysis for net number of trains 
	- Code analyzes and visualizes the number of trains operating on each route line. It starts by loading the dataset cleaned_commuter_rail_trips.csv into a pandas DataFrame. The data is grouped by route_id, and the unique count of trip_id is calculated for each line. This count represents the total number of trains operating on that route. The results are then sorted in descending order based on the number of trains.
	- The code creates a bar chart using Matplotlib to show the results. Each bar represents a route_id, and its height reflects the number of trains. Labels are added above each bar to display the exact train count. The chart includes a title, axis labels, and rotated x-axis ticks to improve clarity.
	- The processed data is saved to a CSV file named net_trains_per_line.csv. This ensures the information is preserved for future use. The bar chart provides a clear visual summary of train operations. Since 2019 is the starting year, it contributes 0% to the data. This step-by-step process highlights the distribution of trains across different lines. 


## Installing Dependencies
To install the dependencies required to run all the analysis and pipelines, run the following command:
```
pip install -r requirements.txt
```

## Downloading the Specific Data
The `download_tables.py` script can be used to download specific or all tables from the MBTA archives for specific years and seasons.
An example command to run the script is 
```
python download_tables.py --seasons Fall Spring Winter Summer --years 2024 2023 2022 2021 2020 --tables stops calendar
```

## Pipeline to Upload Raw Data to BigQuery
The `bigquery_pipeline.py` script can be used to upload all the raw data for each from the MBTA archives to BigQuery.
The command to do so is 
```
python bigquery_pipeline.py --year_range 2019-2024 --project ds-better-city-commuter
```
`--year_range` is used to specify the range of years for which you want the data to be uploaded. `--project` specifies the project ID of your project on Google Cloud Platform.
All the tables for each season are combined for every year and upload to BigQuery. The data for each year is made to be its own dataset with all the tables inside it.

## Pipeline to Upload the Answers for Base Questions on BigQuery
The `bigquery_cleaned_pipeline.py` script is used to upload the dataset corresponding to a specific base question to BigQuery under the `analysis_data` Dataset. 
The command to run it is
```
python3 bigquery_cleaned_pipeline.py --project ds-better-city-commuter --question_num q4
```
The possible values of the argument `--question_num` are 'q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7'. These values correspond to the particular questions in the following manner:
- q1: A breakdown of trains by time of day (early morning, peak morning, midday, afternoon peak, evening, late night)
- q2: Identify / analyze the number of express trains per line
- q3: Do we see incidents of a station changing fare zones?
- q4: If cost info is included in the active GTFS feed & archives, can we compare the changes in fare cost over time?
- q5: How have schedules shifted over time?
- q6: What is the net number of trains operating per line?
- q7: How do travel times vary across different schedules?

Each question mentioned above has a table inside the `analysis_data` dataset in BigQuery.
