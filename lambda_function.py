import boto3
import requests
import snowflake.connector as sf
import os
import toml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def download_inventory_file(url, destination_folder, file_name):
    """
    Download inventory file from the specified URL to the destination folder.
    
    Parameters:
        url (str): The URL of the inventory file.
        destination_folder (str): The folder where the file will be saved.
        file_name (str): The name of the file.
    
    Returns:
        str: The file path of the downloaded file.
    """
    try:
        # Make a GET request to download the file
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        file_path = os.path.join(destination_folder, file_name)
        # Save the file content to the specified destination
        with open(file_path, 'wb') as file:
            file.write(response.content)
        return file_path
    except requests.RequestException as e:
        # Handle any request exceptions (e.g., connection errors, timeouts)
        print(f"Error downloading file: {e}")
        return None

def establish_snowflake_connection():
    """
    Establish connection to Snowflake database.
    
    Returns:
        Snowflake connection object.
    """
    try:
        # Connect to Snowflake using environment variables
        conn = sf.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        return conn
    except sf.errors.Error as e:
        # Handle any Snowflake connection errors
        print(f"Error establishing Snowflake connection: {e}")
        return None

def load_config_from_toml(file_path):
    """
    Load configuration parameters from a TOML file.
    
    Parameters:
        file_path (str): Path to the TOML configuration file.
    
    Returns:
        dict: Configuration parameters.
    """
    try:
        with open(file_path, 'r') as file:
            config = toml.load(file)
        return config
    except Exception as e:
        print(f"Error loading configuration from TOML file: {e}")
        return {}

def create_file_format(cursor, file_format_name):
    """
    Create or replace a file format in Snowflake.
    
    Parameters:
        cursor: Snowflake cursor object.
        file_format_name (str): Name of the file format.
    """
    try:
        # Execute SQL statement to create or replace file format
        cursor.execute(f"CREATE OR REPLACE FILE FORMAT {file_format_name} TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"';")
        print(f"File format {file_format_name} created successfully.")
    except sf.errors.Error as e:
        # Handle any errors during file format creation
        print(f"Error creating file format: {e}")

def create_stage(cursor, stage_name):
    """
    Create or replace a stage in Snowflake.
    
    Parameters:
        cursor: Snowflake cursor object.
        stage_name (str): Name of the stage.
    """
    try:
        # Execute SQL statement to create or replace stage
        cursor.execute(f"CREATE OR REPLACE STAGE {stage_name};")
        print(f"Stage {stage_name} created successfully.")
    except sf.errors.Error as e:
        # Handle any errors during stage creation
        print(f"Error creating stage: {e}")

def upload_file_to_stage(cursor, file_path, stage_name):
    """
    Upload a file to a Snowflake stage.
    
    Parameters:
        cursor: Snowflake cursor object.
        file_path (str): Path of the file to upload.
        stage_name (str): Name of the stage.
    """
    try:
        # Execute SQL statement to upload file to stage
        cursor.execute(f"PUT file://{file_path} @{stage_name};")
        print("File uploaded to stage successfully.")
    except sf.errors.Error as e:
        # Handle any errors during file upload
        print(f"Error uploading file to stage: {e}")

def load_data_into_table(cursor, stage_name, file_format_name, table):
    """
    Load data from a Snowflake stage into a table.
    
    Parameters:
        cursor: Snowflake cursor object.
        stage_name (str): Name of the stage.
        file_format_name (str): Name of the file format.
        table (str): Name of the table to load data into.
    """
    try:
        # Execute SQL statement to load data into table
        cursor.execute(f"COPY INTO {table} FROM @{stage_name} FILE_FORMAT = (FORMAT_NAME = {file_format_name}) ON_ERROR = 'CONTINUE';")
        print("Data loaded into table successfully.")
    except sf.errors.Error as e:
        # Handle any errors during data loading
        print(f"Error loading data into table: {e}")

def main():
    # Load Snowflake configuration from TOML file
    snowflake_config = load_config_from_toml('snowflake_config.toml')

    # Download inventory file from S3
    url = 'https://de-materials-tpcds.s3.ca-central-1.amazonaws.com/inventory.csv'
    destination_folder = '/tmp'
    file_name = 'inventory.csv'
    file_path = download_inventory_file(url, destination_folder, file_name)
    if not file_path:
        print("Exiting due to download error.")
        return
    
    # Establish Snowflake connection
    conn = establish_snowflake_connection()
    if not conn:
        print("Exiting due to Snowflake connection error.")
        return
    
    try:
        # Create or replace file format
        cursor = conn.cursor()
        create_file_format(cursor, snowflake_config['snowflake']['file_format_name'])
        
        # Create or replace stage
        create_stage(cursor, snowflake_config['snowflake']['stage_name'])
        
        # Upload file to stage
        upload_file_to_stage(cursor, file_path, snowflake_config['snowflake']['stage_name'])
        
        # Load data into table
        load_data_into_table(cursor, snowflake_config['snowflake']['stage_name'], snowflake_config['snowflake']['file_format_name'], snowflake_config['snowflake']['table'])
        
    finally:
        # Close Snowflake connection
        conn.close()

    print("Process completed successfully.")

if __name__ == "__main__":
    main()

def lambda_handler(event, context):
    """
    Lambda handler function.
    
    Parameters:
        event: AWS Lambda event data.
        context: AWS Lambda context object.
    
    Returns:
        dict: Lambda response.
    """
    main()
    return {"statusCode": 200, "body": "Process completed successfully"}
