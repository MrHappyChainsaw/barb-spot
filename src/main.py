import argparse
import station_price as bs  # Import your package functions
import pandas as pd

def main(start_date: str, end_date: str):
    """
    Main function to run the Barb Spot package.
    It fetches data from the SQL database for the given date range and displays it.

    Args:
        start_date (str): The start date in YYYY-MM-DD format.
        end_date (str): The end date in YYYY-MM-DD format.
    """
    try:
        # Fetch the data using the package's fetch_data function
        df = bs.fetch_data(start_date, end_date)
        
        # Display the result
        if not df.empty:
            df_filtered = df[df['SalesAreaNo']==36]
            if not df_filtered.empty:
                df_grouped = df_filtered.groupby('DemoNumber')['RatecardImpacts'].sum().reset_index()
                pd.set_option('display.float_format', lambda x: '%.0f' % x)
                print(df_grouped)
            else:
                print(f"No data found for ITV2")
    
            print("Data fetched successfully:")
            # print(df)
        else:
            print(f"No data found between {start_date} and {end_date}.")
    
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # Set up argument parsing for the command-line interface
    parser = argparse.ArgumentParser(description="Barb Spot SQL data fetcher")
    
    # Add command-line arguments for start_date and end_date
    parser.add_argument("start_date", help="The start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="The end date (YYYY-MM-DD)")
    
    # Parse arguments from the command line
    args = parser.parse_args()

    # Call the main function with the parsed arguments
    main(args.start_date, args.end_date)

