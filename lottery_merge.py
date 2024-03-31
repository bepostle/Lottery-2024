import pandas as pd

# Define file paths
file_paths = {
    'apps_raw': '/Users/brent.postlethweight/Desktop/lottery_2024/data_inputs/sf_id_lookup/report1711815103787.csv',
    'yield': '/Users/brent.postlethweight/Desktop/lottery_2024/data_inputs/ds_yield_forecasting/raw_yield_forecasting_applications_tlnd_202403301212.csv',
    'results': '/Users/brent.postlethweight/Desktop/lottery_2024/data_inputs/3.30_lottery_sim_results/Public_30_Results-2024-03-30T160225.csv',
    'full_output': '/Users/brent.postlethweight/Desktop/lottery_2024/merge_file_outputs/merged_data_full.csv',
    'offered_output': '/Users/brent.postlethweight/Desktop/lottery_2024/merge_file_outputs/merged_data_offered.csv',
    'waitlisted_output': '/Users/brent.postlethweight/Desktop/lottery_2024/merge_file_outputs/merged_data_waitlisted.csv'
}

# Read the CSV input files
df_apps_raw = pd.read_csv(file_paths['apps_raw'], encoding='ISO-8859-1')
df_yield = pd.read_csv(file_paths['yield'], usecols=['round_app_id', 'probability_attending_5_days'])
df_results = pd.read_csv(file_paths['results'])

# Define a function to map grade 'K' to 0
def map_grade_to_grade_sort(grade):
    return 0 if grade == 'K' else int(grade)

# Apply the function to create the 'Grade Sort' column
df_apps_raw['Grade Sort'] = df_apps_raw['App Grade'].apply(map_grade_to_grade_sort)

# Filter the apps_raw DataFrame where program rank equals 1
filtered_df_apps_raw = df_apps_raw[df_apps_raw['Program Rank'] == 1]

# Group the filtered DataFrame by round app ID and get the first value of school name
first_choice_school = filtered_df_apps_raw.groupby('18-Digit ID (Round App)')['Program: School Name'].first().reset_index()

# Merge the new column onto the original DataFrame based on round app ID
df_apps_calc = pd.merge(df_apps_raw, first_choice_school.rename(columns={'Program: School Name': 'FIRST CHOICE SCHOOL'}), on='18-Digit ID (Round App)', how='left')

# Merge the apps calc and yield files
df_merged_apps_yield = pd.merge(df_apps_calc, df_yield, left_on='18-Digit ID (Round App)', right_on='round_app_id', how='left')

# Merge the results file onto the merged apps and yield file
df_merged_apps_yield_results = pd.merge(df_merged_apps_yield, df_results, left_on='18-digit ID (hed_application)', right_on='APPLICATION CHOICE ID', how='outer')

# Define a custom function to check if any value in the 'ASSIGNMENT STATUS' column is 'Offer (Seat Held)'
def offer_count(status_column):
    return 1 if (status_column == 'Offer (Seat Held)').any() else 0

# Group df_merged_apps_yield_results by '18-Digit ID (Round App)' and apply the custom function
df_merged_apps_yield_results['OFFER COUNT'] = df_merged_apps_yield_results.groupby('18-Digit ID (Round App)')['ASSIGNMENT STATUS'].transform(offer_count)

# Sort and filter the merged files before writing to CSVs
df_merged_apps_yield_results_sorted = df_merged_apps_yield_results.sort_values(by=['Application ID', 'Program Rank'])
df_offered = df_merged_apps_yield_results[df_merged_apps_yield_results['ASSIGNMENT STATUS'] == 'Offer (Seat Held)']
df_waitlisted = df_merged_apps_yield_results[(df_merged_apps_yield_results['ASSIGNMENT STATUS'] == 'Waitlisted') & (df_merged_apps_yield_results['OFFER COUNT'] == 0) & (df_merged_apps_yield_results['Program Rank'] == 1)]

# Write output files
df_merged_apps_yield_results_sorted.to_csv(file_paths['full_output'], index=False)
df_offered.to_csv(file_paths['offered_output'], index=False)
df_waitlisted.to_csv(file_paths['waitlisted_output'], index=False)

# Get and print the number of rows in each file
num_rows_apps = len(df_apps_raw)
num_rows_yield = len(df_yield)
num_rows_results = len(df_results)
num_rows_full_output_file = len(df_merged_apps_yield_results_sorted)
num_rows_offered_output_file = len(df_offered)
num_rows_waitlisted_output_file = len(df_waitlisted)

print("APPS FILE INPUT:", num_rows_apps)
print("YIELD FILE INPUT:", num_rows_yield)
print("RESULTS FILE INPUT:", num_rows_results)
print("FULL OUTPUT:", num_rows_full_output_file)
print("OFFERED OUTPUT:", num_rows_offered_output_file)
print("WAITLISTED OUTPUT:", num_rows_waitlisted_output_file)
