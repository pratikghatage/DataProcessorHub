import pandas as pd
import argparse

def parse_args():
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description='ETL Script')
    parser.add_argument('input_file') 
    args = parser.parse_args()
    return args

def load_raw_data(input_file):
    """
    Load data from destination

    Args:
        file_path (str): Path to the input file.

    Returns:
        raw_data: Loaded data.
    """
    raw_data = pd.read_csv(input_file, sep='|')
    return raw_data

def check_multivalued_col(raw_data):
    """
    Check the multivalued column in raw data.

    Args:
        raw_data (pd.DataFrame) : Input DataFrame

    Returns:
        multi_val_col: List of columns with multiple values.
    """
    multi_val_col = [col for col in raw_data.columns if any(',' in str(value) for value in raw_data[col])]
    print(f"multi valued columns are", multi_val_col)
    return multi_val_col

def check_singvalued_col(raw_data, multi_val_col):
    """
    Check the single valued column in raw_data.

    Args:
        raw_data (pd.DataFrame) : Input DataFrame
        multi_val_col (list): List of columns with multiple values

    Retuens:
        single_val_col: List of columns with single value
    """
    single_val_col = [col for col in raw_data.columns if col not in multi_val_col]
    
    print(f"single valued columns are", single_val_col)
    return single_val_col

def explode_multival_col(raw_data, multi_val_col):
    """
    Split and explore column with multiple value

    Args:
        raw_data (pd.DataFrame): Input DataFrame
        multi_val_col (list) : List of columns with multiple values.

    Returns:
        processed_data (pd.DataFrame): DataFrame with exploded raws.
    """
    processed_data = raw_data.copy()    
    
    for col in multi_val_col:
        processed_data[col] = raw_data[col].str.split(',')
        processed_data = processed_data.explode(col)

        # Write DataFrame to a pipe-separated values (PSV) file
        processed_data.to_csv('output_file', sep='|', index=False)
    return processed_data


def analysis(processed_data):
    """
    Count of usage of each subnet
    Count of usage of each security group 
    Count of usage of subnet+security group combination (optional)

    Args:
        processed_data (pd.DataFrame): Input processed data
    
    Returns:
        final (pd.DataFrame): The dataframe stored in analysis file in same folder.
    """
    # Count occurrences of each 'subnet' for each 'ip-addr'
    count_each_subnet = processed_data.groupby('ip-addr')['subnet'].nunique().reset_index(name='count_subnet')

    # Count occurrences of each 'security-group' for each 'ip-addr'
    count_each_security_group = processed_data.groupby('ip-addr')['security-group'].nunique().reset_index(name='count_security_group')

    # Merge the two count DataFrames based on 'ip-addr'
    final = pd.merge(count_each_security_group, count_each_subnet, on='ip-addr', how='outer')

    # Write DataFrame to a pipe-separated values (PSV) file
    final.to_csv('analysis', sep='|', index=False)

    # Now, 'count_each_subnet_and_security_group' contains three columns: 'ip-addr', 'count_subnet', and 'count_security_group'
    print("The analysis results are here")
    print(final)


def main():
    """
    Main function for process and modify data like requirement.
    """
    # Parse command line arguments
    args = parse_args()

    # Load data
    raw_data = load_raw_data(args.input_file)

    # Check multivalued column.
    multi_val_col = check_multivalued_col(raw_data)

    # Check singlevalued column.
    check_singvalued_col(raw_data, multi_val_col)

    # Explode the column which multi value
    processed_data = explode_multival_col(raw_data, multi_val_col)

    # Lets do analysis on processed data.
    analysis(processed_data)
     
if __name__ == "__main__":
    print("Simplified assignment for --- IZMO LTD ---")
    main()
