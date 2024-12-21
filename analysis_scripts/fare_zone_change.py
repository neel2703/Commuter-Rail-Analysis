import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns 
import numpy as np 

seasons = ['Spring', 'Summer', 'Fall', 'Winter']

def load_and_filter_seasonal_data(df):
    seasonal_data = []
    for df_season in df:
        filtered_df = df_season[df_season['zone_id'].str.startswith('CR', na=False)]
        seasonal_data.append(filtered_df[filtered_df['location_type'] == 1][['stop_name', 'zone_id']])
    return seasonal_data

def combine_seasonal_data(seasonal_data, year):
    combined_df = pd.concat(seasonal_data).drop_duplicates(subset='stop_name', keep='last')
    combined_df = combined_df.rename(columns={'zone_id': f'zone_{year}'})
    return combined_df

def final_table(df):
    df_2021 = [df[f'stops_2021_{seasons[0]}'], df[f'stops_2021_{seasons[1]}'], df[f'stops_2021_{seasons[2]}'], df[f'stops_2021_{seasons[3]}']]
    df_2022 = [df[f'stops_2022_{seasons[0]}'], df[f'stops_2022_{seasons[1]}'], df[f'stops_2022_{seasons[2]}'], df[f'stops_2022_{seasons[3]}']]
    df_2023 = [df[f'stops_2023_{seasons[0]}'], df[f'stops_2023_{seasons[1]}'], df[f'stops_2023_{seasons[2]}'], df[f'stops_2023_{seasons[3]}']]
    df_2024 = [df[f'stops_2024_{seasons[0]}'], df[f'stops_2024_{seasons[1]}'], df[f'stops_2024_{seasons[2]}'], df[f'stops_2024_{seasons[3]}']]

    stations_2021 = combine_seasonal_data(load_and_filter_seasonal_data(df_2021), 2021)
    stations_2022 = combine_seasonal_data(load_and_filter_seasonal_data(df_2022), 2022)
    stations_2023 = combine_seasonal_data(load_and_filter_seasonal_data(df_2023), 2023)
    stations_2024 = combine_seasonal_data(load_and_filter_seasonal_data(df_2024), 2024)

    merged_data = stations_2021.merge(stations_2022, on='stop_name', how='outer')
    merged_data = merged_data.merge(stations_2023, on='stop_name', how='outer')
    merged_data = merged_data.merge(stations_2024, on='stop_name', how='outer')

    merged_data['zone_change'] = (
        (merged_data['zone_2021'] != merged_data['zone_2022']) |
        (merged_data['zone_2022'] != merged_data['zone_2023']) |
        (merged_data['zone_2023'] != merged_data['zone_2024'])
    )

    changed_stations = merged_data[merged_data['zone_change']]

    return changed_stations

def plot_heatmap(changed_stations):
    df = changed_stations.drop(columns=['zone_change']).fillna("No Zone")
    # df.columns = [col.replace("zone_", "") for col in df.columns]
    df_melted = df.melt(id_vars='stop_name', var_name='year', value_name='zone_id')

    zone_pivot = df_melted.pivot(index='stop_name', columns='year', values='zone_id')
    zone_mapping = {zone: idx for idx, zone in enumerate(zone_pivot.stack().unique())}
    zone_pivot_encoded = zone_pivot.replace(zone_mapping)
    color_palette = sns.color_palette("tab10", n_colors=len(zone_mapping))
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.heatmap(zone_pivot_encoded, 
                annot=zone_pivot, 
                cmap=color_palette, 
                cbar=False, 
                fmt='', 
                linewidths=0.5, 
                linecolor='gray', 
                ax=ax)

    ax.set_title("Zone Changes by Station and Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Station")
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)

    return fig