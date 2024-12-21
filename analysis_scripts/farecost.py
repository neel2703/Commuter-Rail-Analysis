import pandas as pd

# Function to read fare data
def read_fares(fares_df, year, season):
    # fares_data = {}
    # for year in years:
    #     for season in seasons:
    #         file_path = f'{base_path}/{season}{year}/fare_products.txt'
    #         if file_path != skip_file:
    #             fares_data[f'fares_{season}{year}'] = pd.read_csv(file_path, delimiter=',')
    #             fares_data[f'fares_{season}{year}']['season'] = f'{season}'
    #             fares_data[f'fares_{season}{year}']['year'] = f'{year}'
    fares_df['season'] = season
    fares_df['year'] = year
    return fares_df

# Function to read fare leg rules
def read_fare_leg_rules(fares_rules_df):
    return fares_rules_df

# Function to read and process route data
def process_routes(routes_df):
    # routes_data = pd.read_csv(file_path, delimiter=',')
    routes_data = routes_df[routes_df['route_desc'] == 'Commuter Rail']
    routes_data = routes_data.dropna(axis=1, how='any')
    routes_data = routes_data.drop('route_text_color', axis=1, inplace=False)
    return routes_data

# Function to process and merge fare data
def process_fares_and_routes(fares_data, fares_rules_data, routes_data):
    combined_fares_df = pd.concat(fares_data.values(), ignore_index=True)
    combined_fares_rules_df = pd.concat(fares_rules_data.values(), ignore_index=True)
    combined_fares_rules_df = combined_fares_rules_df.drop(columns=['transfer_only', 'from_timeframe_group_id', 'to_timeframe_group_id'])

    combined_fare_result = pd.merge(combined_fares_df, combined_fares_rules_df, on='fare_product_id', how='inner')
    combined_fare_result = pd.merge(combined_fare_result, routes_data, on='network_id', how='left')
    combined_fare_result = combined_fare_result.drop(columns=['agency_id', 'route_type', 'route_sort_order', 'route_color'])

    return combined_fare_result

# Function to filter and process commuter rail data
def filter_commuter_rail_data(combined_fare_result, to_area_id_mapping):
    combined_fare_result_CR = combined_fare_result[combined_fare_result['network_id'].isin(['commuter_rail', 'cape_flyer'])]
    combined_fare_result_CR_cash_1a = combined_fare_result_CR[combined_fare_result_CR['from_area_id'] == 'area_commuter_rail_zone_1a']
    combined_fare_result_CR_cash_1a_drop = combined_fare_result_CR_cash_1a.drop_duplicates()

    filtered_df = combined_fare_result_CR_cash_1a_drop[
        combined_fare_result_CR_cash_1a_drop.apply(
            lambda row: row['to_area_id'] == to_area_id_mapping.get(row['line_id'], row['to_area_id']),
            axis=1
        )
    ]
    return filtered_df

# Function to merge and prepare the final table
def prepare_final_table(filtered_df, combined_fare_result_CR_cash_1a_drop):
    merged_df = filtered_df.merge(
        combined_fare_result_CR_cash_1a_drop[['to_area_id', 'amount']],
        on='to_area_id',
        suffixes=('', '_new'),
        how='left'
    )
    merged_df['amount'] = merged_df['amount_new'].combine_first(merged_df['amount'])
    merged_df = merged_df.drop(columns=['amount_new']).drop_duplicates()
    #print(merged_df[['route_id', 'amount']].drop_duplicates().sort_values(by='amount', ascending=False).head(10))
    merged_df_ppt = merged_df[['route_id', 'amount']]
    merged_df_ppt = merged_df_ppt.drop_duplicates()
    merged_df_ppt = merged_df_ppt.sort_values(by='amount', ascending=False)
    merged_df_ppt = merged_df_ppt.drop_duplicates(subset=['route_id'], keep='first')
    merged_df_ppt = merged_df_ppt.reset_index(drop=True)
    merged_df_ppt['amount'] = merged_df_ppt['amount'].apply(lambda x: f"${x:.2f}")
    return merged_df_ppt

# Main function to execute the process
def main(dataframes):
    years = [2023, 2024]
    seasons = ['Spring', 'Summer', 'Fall', 'Winter']
    # base_path = '/Users/bhuvan/Documents/VS code/DS701 project/Datasets'
    # skip_fares_file = f'{base_path}/Winter2024/fare_products.txt'
    # skip_rules_file = f'{base_path}/Winter2024/fare_leg_rules.txt'
    # routes_file_path = f'{base_path}/Spring2023/routes.txt'

    to_area_id_mapping = {
        'line-CapeFlyer': 'area_cf_zone_hyannis',  # example
        'line-Fairmount': 'area_commuter_rail_zone_2',  # example
        'line-Fitchburg': 'area_commuter_rail_zone_8',
        'line-Worcester': 'area_commuter_rail_zone_8',
        'line-Franklin': 'area_commuter_rail_zone_6',
        'line-Greenbush': 'area_commuter_rail_zone_6',
        'line-Haverhill': 'area_commuter_rail_zone_7',
        'line-Kingston': 'area_commuter_rail_zone_8',
        'line-Lowell': 'area_commuter_rail_zone_6',
        'line-Middleborough': 'area_commuter_rail_zone_8',
        'line-Needham': 'area_commuter_rail_zone_2',
        'line-Newburyport': 'area_commuter_rail_zone_8',
        'line-Providence': 'area_commuter_rail_zone_10',
    }

    fares_data = {}
    fares_rules_data = {}
    routes_df = dataframes['routes_2023_Spring']
    for year in years:
        for season in seasons:
            try:
                fares_data[f'fares_{season}{year}'] = read_fares(dataframes[f'fare_products_{year}_{season}'], year, season)
                fares_rules_data[f'fares_rules_{season}{year}'] = read_fare_leg_rules(dataframes[f'fare_leg_rules_{year}_{season}'])
            except Exception as e:
                continue
    # fares_data = read_fares(years, seasons, base_path, skip_fares_file)
    # fares_rules_data = read_fare_leg_rules(years, seasons, base_path, skip_rules_file)
    routes_data = process_routes(routes_df)

    combined_fare_result = process_fares_and_routes(fares_data, fares_rules_data, routes_data)
    filtered_df = filter_commuter_rail_data(combined_fare_result, to_area_id_mapping)

    final_table = prepare_final_table(filtered_df, filtered_df)
    return final_table

### todo: make changes to the function to accept the dataframes as arguments

# result_table = main()
# print(result_table)
