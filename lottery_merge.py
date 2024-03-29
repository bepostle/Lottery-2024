import pandas as pd

# Specify the full file paths
file_path_apps = '/Users/brent.postlethweight/Desktop/lottery_2024/data_inputs/sf_id_lookup/report1711653633346.csv'
file_path_yield = '/Users/brent.postlethweight/Desktop/lottery_2024/data_inputs/ds_yield_forecasting/raw_yield_forecasting_applications_tlnd_202403271718-1711574328443.csv'
file_path_results = '/Users/brent.postlethweight/Desktop/lottery_2024/Public_30_Results-2024-03-28T135414.csv'

# Read the apps and yield CSV files using the specified file paths
df_apps = pd.read_csv(file_path_apps, usecols=['18-Digit ID (Round App)', 'Application ID', '18-digit ID (hed_application)', 'Application Name'], encoding='ISO-8859-1')
df_yield = pd.read_csv(file_path_yield, usecols=['round_app_id', 'probability_attending_5_days'])

# Merge where 'id' is the column containing the identifier for each row
df_merged_apps_yield = pd.merge(df_apps, df_yield, left_on='18-Digit ID (Round App)', right_on='round_app_id', how='left')

# Read the results CSV file using the specified file paths
df_results = pd.read_csv(file_path_results, usecols=['APPLICATION CHOICE ID', 'STUDENT ID', 'GRADE', 'CHOICE RANK', 'SCHOOL ID', 'SCHOOL NAME', 'CURRENT SIS SCHOOL', 'ASSIGNMENT STATUS', 'WAITLIST RANK', 'SCHOOL PREFERENCE', 'RANDOM NUMBER', 'TIME PROCESSED', 'LOTTERY LOG ID'])

# Merge where 'id' is the column containing the identifier for each row
df_merged_apps_yield_results = pd.merge(df_merged_apps_yield, df_results, left_on='18-digit ID (hed_application)', right_on='APPLICATION CHOICE ID', how='right')

# Specify the full file path to save the merged data
output_file_path = '/Users/brent.postlethweight/Desktop/lottery_2024/merge_file_outputs/merged_data.csv'

# Write the merged DataFrame to a CSV file using the specified file path
df_merged_apps_yield_results.to_csv(output_file_path, index=False)
