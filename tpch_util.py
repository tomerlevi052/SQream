#######################################################################################################################

# Project: Utility for benchmarking TPC-H queries over PostgreSQL database
# Author: Tomer
# Reviewer: Yuval
# Date: 11 - 05 - 2023
# Versions: 1.0

#######################################################################################################################
import psycopg2     # PostgreSQL database adapter for python
import os           # os.listdir() , os.path.join()
import time         # time.time()
import argparse     # Library for parsing command-line arguments.
import datetime     # datetime.datetime.now()
import sys          # sys.exit()

# Printing/Logging with colors
from colorama import init, Fore, Style
# initialize colorama
init()
#######################################################################################################################

# Define PostGreSQL data-base connection details
DB_HOST = "127.0.0.1"
DB_NAME = "mysqreamdb"
DB_USER = "tomer"
DB_PASS = "0546757732"

# Define the path to the directory containing the .tbl files
TBL_DIR_PATH = "/home/tomer/Desktop/tpch_data_extracted"

# Define the path to the requested schema file
SCHEMA_FILE_PATH = "/home/tomer/Desktop/tpch-schema.sql"

# Define the path to the directory containing the query files
QUERIES_DIR_PATH = "/home/tomer/Desktop/tpch5_tpch7"

# Define the name of the result table to create
RESULTS_TABLE_NAME = "tpch_results"
#######################################################################################################################


def connect_to_database():
    try:

        # Establish Connection to database
        db_conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print(f"{Style.BRIGHT}{Fore.GREEN}Connection to {DB_NAME} PostgreSQL database established!{Style.RESET_ALL}")
        print(f"{Style.BRIGHT}{Fore.GREEN}Welcome aboard {DB_USER}!{Style.RESET_ALL} \n")

        # Create a cursor object
        cursor = db_conn.cursor()
        print(Fore.LIGHTMAGENTA_EX + "Cursor object created! \n" + Style.RESET_ALL)

    except psycopg2.OperationalError as exception_object:
        sys.exit(f"{Fore.RED}Could not connect to the database: {exception_object}{Style.RESET_ALL}")

    return db_conn, cursor

#######################################################################################################################


def create_new_schema():
    """
    Reads the schema file and creates tables in the database.
    Drops existing tables with the same name if any.
    """
    try:

        # Create Schema -> Read the schema file adn create the tables it consists of
        f_scheme_string = open(SCHEMA_FILE_PATH, "r")

        print(Fore.LIGHTCYAN_EX + "Looking for replacing existing tables..." + Style.RESET_ALL)
        # Loop through each line in the schema file to check for an already exists table with the same name
        for curr_line in f_scheme_string:

            # Check if there is a header of a "table struct" in the current iterated line
            if "CREATE TABLE" in curr_line:
                # Extracts the name of the table -> split the line into a list of words and select the third word
                # Choose the third word substring index from the substrings list
                # Strip the chosen word from any whitespaces
                table_token = curr_line.split()[2].strip()
                cursor.execute(f"DROP TABLE IF EXISTS {table_token}")
                print(f"Table name: {Fore.LIGHTWHITE_EX}-{table_token}-  dropped and will be replaced.. {Style.RESET_ALL}")

        # Reset the file pointer back to the beginning of the file
        f_scheme_string.seek(0)

        # Create the tables from the schema file
        cursor.execute(f_scheme_string.read())
        print(Fore.GREEN + "New schema created successfully!" + Style.RESET_ALL)  # add scheme name to the print

        # Commit/Save the changes to the database
        db_conn.commit()

    except psycopg2.OperationalError as exception_object:
        sys.exit(f"{Fore.RED}Error in creating new schema: {exception_object}{Style.RESET_ALL}")


#######################################################################################################################


def load_data_to_base():

    print("Loading data into the schema...")

    try:

        # Loop through each .tbl file in the directory
        for curr_filename in os.listdir(TBL_DIR_PATH):  # os.listdir makes a list from the files in dir

            if curr_filename.endswith(".tbl"):
                # Modify the name of each file and remove/cut the ".tbl" extension
                table_name = curr_filename[:-4]

                # Construct the path to the tbl file
                tbl_curr_file_path = os.path.join(TBL_DIR_PATH, curr_filename)  # os.path_join takes two or more arguments and joins them

                # psycopg2's copy_from() method to copy the data from the tbl file
                # To the corresponding table in the database
                with open(tbl_curr_file_path, "r") as curr_file_to_copy:
                    cursor.copy_from(curr_file_to_copy, table_name, sep='|') #delimiter separation
                    print(f"{curr_filename} data is being loaded to {table_name} table...")

                    # Commit the changes to the database
                    db_conn.commit()

    except psycopg2.OperationalError as exception_object:
        sys.exit(f"{Fore.RED}Error in loading data: {exception_object}{Style.RESET_ALL}")

#######################################################################################################################


def run_performance_benchmarking():

    print("Executing queries...")

    try:
        # Loop through each query file in the directory
        for query_file_name in os.listdir(QUERIES_DIR_PATH):

            if query_file_name.endswith(".sql"):

                # Construct the path to the query file
                query_file_path = os.path.join(QUERIES_DIR_PATH, query_file_name)

                # Read the query from the file
                with open(query_file_path, "r") as query_file:
                    curr_query = query_file.read()

                # Execute the query and calculate the execution time
                start_time = time.time()
                cursor.execute(curr_query)
                end_time = time.time()
                execute_time = end_time - start_time  # end_time - start_time:.2f? / maybe milliseconds?

                # Save the current query running date time
                run_datetime = datetime.datetime.now()

                # Check if saving the results in database results table functionality is requested
                if args.run_benchmark and args.save_results:

                    # Store the needed result table values in a list
                    query_values = run_datetime, query_file_name, execute_time

                    # Insert the current query results into database results table
                    insert_benchmarking_results(query_values)

                else:

                    # Print the query execution time
                    print(f"Query {query_file_name} executed in - {execute_time} seconds")

    except psycopg2.OperationalError as exception_object:
        sys.exit(f"{Fore.RED}Error in running performance benchmarking: {exception_object}{Style.RESET_ALL}")


#######################################################################################################################

def create_results_table():
    """
    Creates results table in database.
    Drops existing table with the same name if any.
    """

    try:

        # Check if table exists
        cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{RESULTS_TABLE_NAME}');")
        table_exists = cursor.fetchone()[0]

        # case met? -> just return without creating it
        if table_exists:
            return

        else:
            # Define the SQL command to create the table with the desired columns and data types constraints
            create_table_cmd = f"""
            CREATE TABLE {RESULTS_TABLE_NAME} (
            run_datetime TIMESTAMP,
            tcph_query_name  TEXT,
            benchmark_result NUMERIC
            );
            """

            # Create the results table
            cursor.execute(create_table_cmd)

            # Commit the changes to the database
            db_conn.commit()
            print(f"Table {RESULTS_TABLE_NAME} created!")

    except psycopg2.OperationalError as exception_object:
        sys.exit(f"{Fore.RED}Error in creating new results table: {exception_object}{Style.RESET_ALL}")


def insert_benchmarking_results(query_values):

    try:

        print(f"Saving {query_values[1]} results to: {RESULTS_TABLE_NAME}")

        # Define a query cmd
        query = f"INSERT INTO {RESULTS_TABLE_NAME} (run_datetime, tcph_query_name, benchmark_result) VALUES " \
                f"('{query_values[0]}', '{query_values[1]}', '{query_values[2]}')"

        # Insert the current operated query results into the corresponding fields in results table
        cursor.execute(query)

        # Commit the changes to the database
        db_conn.commit()

    except psycopg2.OperationalError as exception_object:
        sys.exit(f"{Fore.RED}Error in populating results table: {exception_object}{Style.RESET_ALL}")

#######################################################################################################################


def fetch_results():
    try:

        print(f"Fetching results from {RESULTS_TABLE_NAME} table..")
        # Execute a SELECT statement to get all saved query results from the table
        cursor.execute(f"SELECT * FROM {RESULTS_TABLE_NAME}")

        # Fetch all the results and store them in a list
        rows = cursor.fetchall()

        print("Printing results to the screen..")
        # Iterate the list and print the results fetched to the screen
        for each_row in rows:
            print(each_row)

    except psycopg2.OperationalError as exception_object:
        sys.exit(f"{Fore.RED}Error in fetching results table: {exception_object}{Style.RESET_ALL}")

#######################################################################################################################


if __name__ == "__main__":

    # Establish connection to the database
    db_conn, cursor = connect_to_database()

    # Create the argument parser object with description
    parser = argparse.ArgumentParser(description='Required command-line script functionalities')

    # Define the required command line optional arguments and create "help" dictionary
    parser.add_argument('--create-schema', action='store_true', help="""Reads the schema file and creates tables in the 
    database. Drops existing tables with the same name if any.""")
    parser.add_argument('--load-data', action='store_true', help='Loads the TPCH .tbl files to corresponding tables')
    parser.add_argument('--run-benchmark', action='store_true', help='Run queries and output the query execution time')
    parser.add_argument('--save-results', action='store_true', help="""Benchmark results will be saved to tpch_results 
    table in database""")
    parser.add_argument('--fetch-results', action='store_true', help="""Fetch the results saved in results table and 
    print it""")

    # Parse the command-line arguments passed to the script and return their stored values (parse_args returnes a namespace object)
    args = parser.parse_args()

    # Trigger the functionality combo corresponding to the cmd args received

    # --create-schema
    if args.create_schema:
        create_new_schema()
        # reset all - delete result - table

    # --load-data
    elif args.load_data:
        load_data_to_base()

    # --run-benchmark -- save-results
    elif args.run_benchmark and args.save_results:
        create_results_table()
        run_performance_benchmarking()

    # --run-benchmark
    elif args.run_benchmark:
        run_performance_benchmarking()

    # ! --save-results ! (error when inserted alone)
    elif args.save_results:
        print("--save-results can't be called alone. Use it together with --run-benchmark.")

    # --fetch-results
    elif args.fetch_results:
        fetch_results()

    # other options
    else:
        print("No argument provided. Use --run-benchmark, --save-results, or --fetch-results.")

    try:

        # Clean up and close the cursor and connection to the database
        cursor.close()
        db_conn.close()
        print('\n')
        print(f"{Style.BRIGHT}{Fore.LIGHTRED_EX}Disconnected from {DB_NAME}..")
        print(f"{Style.BRIGHT}{Fore.LIGHTGREEN_EX}See you soon {DB_USER}!\n")

    except psycopg2.OperationalError as exception_object:
        sys.exit(f"{Fore.RED}Error in cleaning up: {exception_object}{Style.RESET_ALL}")

