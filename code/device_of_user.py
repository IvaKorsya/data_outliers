from google.colab import drive
import pandas as pd
import matplotlib.pyplot as plt

def load_data(file_path):
    """Loads data from a Parquet file and returns a Pandas DataFrame."""
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        print(f"Error loading {e}")
        return None

def analyze_device_usage(df):
    """Analyzes device usage based on ua_is_tablet, ua_is_pc, and ua_is_mobile columns
       and visualizes the proportions, including multi-device users.
    """
    # Check if the relevant columns exist. If not, print an error message and return early.
    required_columns = ['ua_is_tablet', 'ua_is_pc', 'ua_is_mobile', 'randPAS_session_id']
    if not all(col in df.columns for col in required_columns):
        print(f"Error: DataFrame must contain columns {required_columns} for device analysis.")
        return

    # 1. Check for potential data inconsistencies (multiple device flags set to 1 within the same row)
    simultaneous_device_mask = (df[['ua_is_tablet', 'ua_is_pc', 'ua_is_mobile']].sum(axis=1) > 1)

    if simultaneous_device_mask.any():
        print("Warning: Some rows have more than one device flag set to 1.  This is likely bad data.")
        print(df[simultaneous_device_mask]) #Display the offending row(s)

    # 2. Identify users using multiple devices
    multi_device_users = df.groupby('randPAS_session_id')[['ua_is_tablet', 'ua_is_pc', 'ua_is_mobile']].sum()
    multi_device_users['device_count'] = multi_device_users.sum(axis=1)
    multi_device_users = multi_device_users[multi_device_users['device_count'] > 1]
    num_multi_device_users = len(multi_device_users)

    # 3. Calculate Single Device Proportions (tablet, pc, mobile users that *only* use that device)
    single_device_users = df.groupby('randPAS_session_id')[['ua_is_tablet', 'ua_is_pc', 'ua_is_mobile']].max() #max as if ever 1 they count as this.  sum will double count users in rows with multiple events from the same device
    single_device_proportions = single_device_users.sum() / df['randPAS_session_id'].nunique() #Divided by the total *unique* user passport IDs


    # Add "Multi-Device" category to proportions for visualization purposes

    single_device_proportions['Multi-Device'] = num_multi_device_users / df['randPAS_session_id'].nunique()

    # Reorder for more readable chart
    single_device_proportions = single_device_proportions[['ua_is_pc', 'ua_is_mobile', 'ua_is_tablet', 'Multi-Device']]

    # 4. Visualize Device Proportions
    plt.figure(figsize=(8, 6))
    single_device_proportions.plot(kind='pie', autopct='%1.1f%%', startangle=140, colors=['skyblue', 'lightgreen', 'lightcoral', 'orange'])  # more colorful plot, add orange for Multi-Device
    plt.title('Device Usage Proportions (Including Multi-Device Users)', fontsize=16)
    plt.ylabel('')  # Remove the default y-axis label
    plt.tight_layout()
    plt.show()



def main(file_path):
    """Main function to load data and analyze device usage."""
    df = load_data(file_path)

    if df is not None: #Proceed only if loading the data was successful.
        analyze_device_usage(df) #Analyze, check for errors, compute proportions and visualize.


# Example usage (Colab specific)
drive.mount('/content/drive', force_remount=True)  # Connect to Google Drive
file_path = '/content/drive/MyDrive/dataset/data_2024-10-04.parquet'  # Replace with your actual path
main(file_path)