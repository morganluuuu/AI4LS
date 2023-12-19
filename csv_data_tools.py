import pandas as pd 
import glob
import re
import os
import gc
import sqlite3
from memory_profiler import profile



INPUT_CSV_PATH = f'{os.path.dirname(os.path.abspath(__file__))}/input_csv/*.csv'
OUTPUT_CSV_PATH = f'{os.path.dirname(os.path.abspath(__file__))}/output_csv/concatenated_csv.csv'
# Define the chunksize for reading CSV files
CHUNKSIZE = 10000 # 10k rows at one time
# Add more columns as needed
SELECTED_COLS = "POINTID|SURVEY_YEAR|GPS_LAT|GPS_LONG|pH_H2O|pH_CaCl2|EC|OC|CaCO3|P|N|K|LC0_Desc|LC1_Desc|LU1_Desc|Un-/Managed_LU|BD 0-20|Coarse|Clay|Sand|Silt|EROSION_PRESENT"
    
# Define a dictionary to map old column names to new ones
COL_NAME_MAPPING = {
        'POINT_ID': 'POINTID',
        'Point_ID': 'POINTID',
        'pH_in_H2O': 'pH_H2O',
        'pH(H2O)': 'pH_H2O',
        'pH_in_CaCl': 'pH_CaCl2',
        'pH(CaCl2)': 'pH_CaCl2',
        'P_x': 'P',
        'CEC': 'EC',
        'coarse': 'Coarse',
        'clay' : 'Clay',
        'silt' : 'Silt',
        'sand' : 'Sand',
        'TH_LAT' : 'GPS_LAT',
        'TH_LONG': 'GPS_LONG',
        # Add more mappings as needed
    }

def update_gps_val(input_csv_path_a : str,
                   input_csv_path_b : str, 
                   output_csv_path : str = OUTPUT_CSV_PATH) -> None:
    '''
    Replicate the 'GPS_LAT' and 'GPS_LONG' values, contingent upon their existence, by correlating the 'POINTID' across two CSV files.
    
    Parameters:
        input_csv_path (str): The path to the input CSV file.
        output_csv_path (str): The path to the output CSV file.
    Returns:
        None: This function does not return anything.
    Raises:
        Exception: If there is an error while updating GPS coordinates.
    '''
    try:
        # Read the input CSV files
        csv_with_data = pd.read_csv(input_csv_path_a)
        csv_missing_data = pd.read_csv(input_csv_path_b)
        
        if not csv_with_data or not csv_missing_data:
            raise FileNotFoundError("No CSV files found. Please check the input path... ")
        else:
            # 1. Drop duplicates in 'POINTID' column before setting it as index
            csv_with_data = csv_with_data.drop_duplicates(subset='POINTID')
            csv_missing_data = csv_missing_data.drop_duplicates(subset='POINTID')

            # 2. Set 'POINTID' as the index for csv_missing_data and csv_with_data for easier data manipulation
            csv_with_data.set_index('POINTID', inplace=True)
            csv_missing_data.set_index('POINTID', inplace=True)

            # 3. Update the 'GPS_LAT' and 'GPS_LONG' columns in csv_missing_data with the values from csv_with_data
            csv_missing_data.update(csv_with_data[['GPS_LAT', 'GPS_LONG']])

            # 4. Reset the index for csv_missing_data
            csv_missing_data.reset_index(inplace=True)
            
            # 5. Save the updated DataFrame to the output CSV file
            csv_missing_data.to_csv(output_csv_path, index=False)
            print(" CSV file created successfully!...")
            return None
        
    except Exception as e:
        print(f" Error while updating GPS coordinates: {e}")


@profile
def unify_col_name(input_path: str = INPUT_CSV_PATH, 
                   column_mapping: dict = COL_NAME_MAPPING) -> None:
    '''
    Renames columns in multiple CSV files to solve the problem of discrepancies in column names.
    Args:
        input_path (str): The path to the directory containing the CSV files. Defaults to INPUT_CSV_PATH.
        column_mapping (dict): A dictionary mapping the old column names to the new column names. 
                               Defaults to COL_NAME_MAPPING.
    Returns:
        None: This function does not return anything.
    Raises:
        Exception: If no CSV files are found at the specified input path.
    '''
    try:
        # Get a list of all CSV files
        csv_list = glob.glob(input_path)
        if not csv_list:
            raise FileNotFoundError("No CSV files found. Please check the input path... ")
        else:
            print(f" {len(csv_list)} CSV files found for rename...")
            # Process each file
            for file in csv_list:
                # Read the file
                df = pd.read_csv(file,low_memory=False)

                # Rename the columns
                df.rename(columns = column_mapping, inplace = True)

                # Write the data back to the file
                df.to_csv(file, index=False)

                # Release memory
                del df
                gc.collect()
            print(" Renamed CSV columns successfully!...")
    except Exception as e:
        print(f" Error while renaming CSV columns : {e}")

@profile
def input_csv(input_path: str = INPUT_CSV_PATH, 
              usecols: str = SELECTED_COLS,
              chunksize: int = CHUNKSIZE) -> list:
    """
    Read CSVs to a DataFrame list.

    Parameters:
        input_path (str): Path to the CSV files. Defaults to INPUT_CSV_PATH.
        usecols (str): Columns to be read from the CSV files. Defaults to SELECTED_COLS.
        chunksize (int): Number of rows to be read at a time. Defaults to CHUNKSIZE.

    Returns:
        list: List of DataFrames containing the CSV data.

    Raises:
        FileNotFoundError: If no CSV files are found at the specified input path.
    """
    try:
        # Get a list of all CSV files
        csv_list = glob.glob(input_path)
        if not csv_list:
            raise FileNotFoundError("No CSV files found. Please check the input path... ")
        else:
            print(f" {len(csv_list)} CSV files found for read...")
            # Read the CSV files
            df_list = [chunk for file in csv_list for chunk in pd.read_csv(
                        # I/O
                        file, 
                        memory_map = True, 
                        chunksize = chunksize, 
                        compression = None, 
                        encoding = 'utf-8', 
                        engine = 'python',
                        # Columns
                        sep = None, 
                        header = 'infer', 
                        index_col = False, 
                        usecols = lambda col: re.match(r'' + usecols, col),
                        converters = None,
                        # Content
                        escapechar = None,
                        keep_default_na = True, 
                        na_filter = True, 
                        skip_blank_lines = False, 
                        )]   
            print(" Read CSV files successfully...")
            return df_list
            
    except Exception as e:
        print(f" Error while reading CSV : {e}")

@profile
def concat_csv(df: list, 
               usecols: str = SELECTED_COLS) -> pd.DataFrame:
    """
    Concatenates a list of dataframes and selects specified columns.

    Parameters:
        df (list): A list of pandas dataframes to be concatenated.
        usecols (str): A string specifying the columns to be selected. Default is SELECTED_COLS.

    Returns:
        pd.DataFrame: The concatenated dataframe with selected columns.

    Raises:
        Exception: If an error occurs while combining the dataframes.
    """
    try:
        # Concatenate the dataframes
        concat_df = pd.concat(df, 
                              ignore_index = True # Reset the index and ave a new continuous index.
                              )
        # Reselect the columns
        reselected_df = concat_df[usecols.split('|')]
        # Add a name to the index
        reselected_df.index.name = 'ID'
        print(" Concat dataframes successfully!...")
        return reselected_df

    except Exception as e:
        print("Error while combining CSV:", str(e))
# @profile
def output_csv(df, chunksize: int = CHUNKSIZE, 
               output_path: str = OUTPUT_CSV_PATH) -> None:
    """
    Write the combined DataFrame to a new CSV file.

    Parameters:
    - df: pandas.DataFrame
        The DataFrame to be written to the CSV file.
    - chunksize: int, optional
        The number of rows to write at a time. Default is CHUNKSIZE.
    - output_path: str, optional
        The path to the output CSV file. Default is OUTPUT_CSV_PATH.

    """
    try:
        df.to_csv(
            path_or_buf = output_path,
            mode = 'w', # 'w', 'a', 'x'
            encoding = 'utf-8', 
            chunksize = chunksize,)
        
        # Delete the DataFrame to save memory
        del df
        gc.collect()
        print(" CSV file created successfully!...")
    except PermissionError: 
        print(" Permission denied. Unable to write the CSV file...")
    except Exception as e:
        print(" Error while writing to CSV: ", str(e))
        
def output_sqlite(df, db_name: str = 'combined_csv.db', 
                  table_name: str = 'combined_csv') -> None:
    """
    Write the combined DataFrame to a new SQLite database.

    Parameters:
    - df: pandas.DataFrame
        The DataFrame to be written to the SQLite database.
    - db_name: str, optional
        The name of the SQLite database. Default is 'combined_csv.db'.
    - table_name: str, optional
        The name of the table in the SQLite database. Default is 'combined_csv'.

    """
    try:
        # Create a connection to the database
        conn = sqlite3.connect(db_name)
        # Write the DataFrame to the database
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        # Close the connection
        conn.close()
        # Delete the DataFrame to save memory
        del df
        gc.collect()
        print(" SQLite database created successfully!...")
    except Exception as e:
        print(" Error while writing to SQLite database: ", str(e))
        
def main():
    unify_col_name()
    update_gps_vol()
    output_csv(concat_csv(input_csv()))
        
        
        
        
if __name__ == '__main__':
    main()
        
        
