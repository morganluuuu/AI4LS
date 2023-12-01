import pandas as pd 
import glob
import re
import os
import gc
 
INPUT_CSV_PATH = f'{os.path.dirname(os.path.abspath(__file__))}/input_csv/*.csv'
OUTPUT_CSV_PATH = f'{os.path.dirname(os.path.abspath(__file__))}/output_csv/combined_csv.csv'

# Define the chunksize for reading CSV files
CHUNKSIZE = 10000 
# Add more columns as needed
SELECTED_COLS = "ID|pH_H2O|pH_CaCl2|EC|OC|CaCO3|P|N|K|LC1_Desc|LU1_Desc"
    
# Define a dictionary to map old column names to new ones
COL_NAME_MAPPING = {
        'POINT_ID': 'ID',
        'Point_ID': 'ID',
        'POINTID': 'ID',
        'pH_in_H2O': 'pH_H2O',
        'pH(H2O)': 'pH_H2O',
        'pH_in_CaCl': 'pH_CaCl2',
        'pH(CaCl2)': 'pH_CaCl2',
        'P_x': 'P',
        'CEC': 'EC',
        # Add more mappings as needed
    }

def unify_col_name(input_path: str = INPUT_CSV_PATH, 
                   column_mapping: dict = COL_NAME_MAPPING) -> None:
    '''Rename the columns of all CSV files in the input path'''
    try:
        # Get a list of all CSV files
        csv_list = glob.glob(input_path)
        if not csv_list:
            raise Exception("No CSV files found. Please check the input path... ")
        else:
            print(f" {len(csv_list)} CSV files found for rename...")
            # Process each file
            for file in csv_list:
                # Read the file
                df = pd.read_csv(file)

                # Rename the columns
                df.rename(columns=column_mapping, inplace=True)

                # Write the data back to the file
                df.to_csv(file, index=False)
                
            # Delete the DataFrame to save memory
            del df
            gc.collect()
            print(" Renamed CSV columns successfully!...")
            
    except Exception as e:
        print(f" Error while renaming CSV columns : {e}")


def input_csv(input_path: str = INPUT_CSV_PATH, 
              usecols: str = SELECTED_COLS,
              chunksize: int = CHUNKSIZE) -> list:
    """ Read CSVs to a DataFrame list """
    try:
        # Get a list of all CSV files
        csv_list = glob.glob(input_path)
        if not csv_list:
            raise Exception("No CSV files found. Please check the input path... ")
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
                        index_col = None, 
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


def combine_csv(df: list, usecols: str = SELECTED_COLS) -> pd.DataFrame:
    """ Concatenate all the dataframes in the list """
    try:
        combined_df = pd.concat(df, ignore_index=True)
        # Reselect the columns
        reselected_df = combined_df[usecols.split('|')]
        print(" Combined dataframes successfully!...")
        return reselected_df
    
    except Exception as e:
        print(" Error while combined to CSV: ", str(e))
        
    
def output_csv(df, chunksize: int = CHUNKSIZE, 
               output_path: str = OUTPUT_CSV_PATH) -> None:
    """ Write the combined DataFrame to a new CSV file """
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
        
        
if __name__ == '__main__':
    # Create output folders if they don't exist
    if not os.path.exists(os.path.dirname(INPUT_CSV_PATH)):
        os.makedirs(os.path.dirname(INPUT_CSV_PATH))
    
    unify_col_name()
    output_csv(combine_csv(input_csv()))
