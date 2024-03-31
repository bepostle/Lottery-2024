import pandas as pd

# Specify the full input file paths
file_path_apps_raw = '/Users/brent.postlethweight/Desktop/lottery_2024/data_inputs/sf_id_lookup/report1711815103787.csv'
file_path_yield = '/Users/brent.postlethweight/Desktop/lottery_2024/data_inputs/ds_yield_forecasting/raw_yield_forecasting_applications_tlnd_202403301212.csv'
file_path_results = '/Users/brent.postlethweight/Desktop/lottery_2024/3.30_lottery_sim_results/Public_30_Results-2024-03-30T160225.csv'


# Specify the full output file paths
full_results_output_file_path = '/Users/brent.postlethweight/Desktop/lottery_2024/merge_file_outputs/merged_data_full.csv'
offered_results_output_file_path = '/Users/brent.postlethweight/Desktop/lottery_2024/merge_file_outputs/merged_data_offered.csv'
waitlisted_results_output_file_path = '/Users/brent.postlethweight/Desktop/lottery_2024/merge_file_outputs/merged_data_waitlisted.csv'


# Read the apps CSV file using the specified file paths
df_apps_raw = pd.read_csv(file_path_apps_raw, encoding='ISO-8859-1')

# Define a function to map grades to grade sorts
def map_grade_to_grade_sort(grade):
    if grade == 'K':
        return 0
    else:
        return int(grade)

# Apply the function to create the 'Grade Sort' column
df_apps_raw['Grade Sort'] = df_apps_raw['App Grade'].apply(map_grade_to_grade_sort)

# Filter the apps DataFrame where program rank equals 1
filtered_df_apps_raw = df_apps_raw[df_apps_raw['Program Rank'] == 1]

# Group the filtered DataFrame by round app ID and get the first value of school name
first_choice_school = filtered_df_apps_raw.groupby('18-Digit ID (Round App)')['Program: School Name'].first().reset_index()

# Merge the new column onto the original DataFrame based on round app ID
df_apps_calc = pd.merge(df_apps_raw, first_choice_school.rename(columns={'Program: School Name': 'FIRST CHOICE SCHOOL'}), on='18-Digit ID (Round App)', how='left')

# Read the yield CSV file using the specified file paths
df_yield = pd.read_csv(file_path_yield, usecols=['round_app_id', 'probability_attending_5_days'])

# Merge the apps calc and yield files where 'id' is the column containing the identifier for each row
df_merged_apps_yield = pd.merge(df_apps_calc, df_yield, left_on='18-Digit ID (Round App)', right_on='round_app_id', how='left')

# Read the results CSV file using the specified file paths
df_results = pd.read_csv(file_path_results, usecols=['APPLICATION CHOICE ID', 'STUDENT ID', 'GRADE', 'CHOICE RANK', 'SCHOOL ID', 'SCHOOL NAME', 'CURRENT SIS SCHOOL', 'ASSIGNMENT STATUS', 'WAITLIST RANK', 'SCHOOL PREFERENCE', 'RANDOM NUMBER', 'TIME PROCESSED', 'LOTTERY LOG ID'])

# Merge the results file onto the merged apps and yield file
df_merged_apps_yield_results = pd.merge(df_merged_apps_yield, df_results, left_on='18-digit ID (hed_application)', right_on='APPLICATION CHOICE ID', how='outer')

# Define a custom function to check if any value in the 'ASSIGNMENT STATUS' column is 'Offer (Seat Held)'
def offer_count(status_column):
    if (status_column == 'Offer (Seat Held)').any():
        return 1
    else:
        return 0

# Group df_merged_apps_yield_results by '18-Digit ID (Round App)' and apply the custom function
df_merged_apps_yield_results['OFFER COUNT'] = df_merged_apps_yield_results.groupby('18-Digit ID (Round App)')['ASSIGNMENT STATUS'].transform(offer_count)

# Sort the merged DataFrame by 'Application ID'
df_merged_apps_yield_results_sorted = df_merged_apps_yield_results.sort_values(by=['Application ID', 'Program Rank'])

# Write the merged DataFrame to a CSV file using the specified file path
df_merged_apps_yield_results_sorted.to_csv(full_results_output_file_path, index=False)

# Filter the merged DataFrame on 'ASSIGNMENT STATUS' where it equals 'Offer (Seat Held)'
df_offered = df_merged_apps_yield_results[df_merged_apps_yield_results['ASSIGNMENT STATUS'] == 'Offer (Seat Held)']

# Save the filtered DataFrame to a CSV file
df_offered.to_csv(offered_results_output_file_path, index=False)

# Filter the merged DataFrame on 'ASSIGNMENT STATUS' where it equals 'Waitlisted'
df_waitlisted = df_merged_apps_yield_results[(df_merged_apps_yield_results['ASSIGNMENT STATUS'] == 'Waitlisted') & (df_merged_apps_yield_results['OFFER COUNT'] == 0) & (df_merged_apps_yield_results['Program Rank'] == 1)]

# Save the filtered DataFrame to a CSV file
df_waitlisted.to_csv(waitlisted_results_output_file_path, index=False)


# Get and print the number of rows in the apps file:
num_rows_apps = df_apps_raw.shape[0]
print("Apps file rows:", num_rows_apps)

# Get and print the number of rows in the yield file:
num_rows_yield = df_yield.shape[0]
print("Yield file rows:", num_rows_yield)

# Get and print the number of rows in the results file:
num_rows_results = df_results.shape[0]
print("Results file rows:", num_rows_results)



