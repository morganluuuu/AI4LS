import pandas as pd 
import glob
import re
import os
 
INPUT_CSV_PATH = f'{os.path.dirname(os.path.abspath(__file__))}/input_csv/*.csv'
OUTPUT_CSV_PATH = f'{os.path.dirname(os.path.abspath(__file__))}/output_csv/combined_csv.csv'
CHUNKSIZE = 10000 
SELECTED_COLS = """
    pH*|pH|p

    """

def input_csv(input_path: str = INPUT_CSV_PATH, 
              selected_cols: str = SELECTED_COLS,
              chunksize: int = CHUNKSIZE) -> list:
    """ Read CSVs to a DataFrame list """
    try:
        csv_list = glob.glob(input_path)
        if not csv_list:
            raise Exception("No CSV files found. Please check the input path... ")
        else:
            print(f" {len(csv_list)} CSV files found...")
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
                        usecols = lambda col: re.match(re.compile(r'' + selected_cols,re.IGNORECASE), col),
                        converters = None,
                        # Content
                        escapechar = None,
                        keep_default_na = True, 
                        na_filter = True, 
                        skip_blank_lines = True, 
                        )]  
            print(" Read CSV files successfully...")
            return df_list
            
    except Exception as e:
        print(f" Error while reading CSV : {e}")


def combine_csv(df) -> pd.DataFrame:
    """ Concatenate all the dataframes in the list """
    try:
        combined_df = pd.concat(df, ignore_index=True)
        print(" Combined dataframes successfully!...")
        return combined_df
    
    except Exception as e:
        print("Error while combined to CSV: ", str(e))
    
    
def output_csv(df, chunksize: int = CHUNKSIZE, 
               output_path: str = OUTPUT_CSV_PATH) -> None:
    """ Write the combined DataFrame to a new CSV file """
    try:
        df.to_csv(
            path_or_buf = output_path,
            mode = 'w', # 'w', 'a', 'x'
            encoding = 'utf-8', 
            chunksize = chunksize,)
        print(" CSV file created successfully!...")
    except PermissionError: 
        print(" Permission denied. Unable to write the CSV file...")
    except Exception as e:
        print(" Error while writing to CSV: ", str(e))
        
        
if __name__ == '__main__':
    output_csv(combine_csv(input_csv()))