import datetime
import csv
import psycopg2
from enum import IntEnum
from datetime import timedelta
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Enum for days of the week for better readability and to avoid hardcoding
class WeekDay(IntEnum):
    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6

def get_db_connection():
    """
    Establish a connection to the PostgreSQL database using credentials from environment variables.
    Returns a connection object.
    """
    dbname = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    pwd = os.getenv('DB_PASSWORD')
    hostname = os.getenv('DB_HOST')
    portnumber = os.getenv('DB_PORT')

    if not all([dbname, username, pwd, hostname, portnumber]):
        raise ValueError("Database credentials are not fully set in the .env file.")

    return psycopg2.connect(database=dbname, user=username, password=pwd, host=hostname, port=portnumber)

def fetch_db_details(query):
    """
    Execute a given SQL query and fetch all results.
    
    Parameters:
    query (str): SQL query to be executed.
    
    Returns:
    list: A list of tuples containing the result set.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

def load_csv_data(filepath):
    """
    Load data from a CSV file.
    
    Parameters:
    filepath (str): Path to the CSV file.
    
    Returns:
    list: A list of rows from the CSV file, excluding the header.
    """
    with open(filepath) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader)  # Skip header
        return list(csv_reader)

def process_stage_details(stage_details):
    """
    Process the stage details to split multiple parts into separate entries.
    
    Parameters:
    stage_details (list): List of tuples where each tuple contains job name and parts.
    
    Returns:
    list: A list of tuples with job names and individual parts.
    """
    processed_details = []
    for row in stage_details:
        parts = row[1].split(",")
        processed_details.extend((row[0], part) for part in parts)
    return processed_details

def check_job_status(job_details, db_details, date_match):
    """
    Check the status of jobs against the database details.
    
    Parameters:
    job_details (list): List of tuples containing job names and conditions.
    db_details (list): Database results containing job names, statuses, and execution counts.
    date_match (str): A string indicating the date to be checked (e.g., 'Yesterday', 'Today').
    
    Returns:
    int: The count of jobs that did not meet the expected conditions.
    """
    result = 0
    for job_name, condition in job_details:
        matching_db = [data for data in db_details if data[0] == job_name]
        if not matching_db:
            print(f"This job was not launched on {date_match}: {job_name}")
            result += 1
        else:
            for data in matching_db:
                if data[1] == 'COMPLETED':
                    if int(condition) == int(data[2]):
                        print(f"This job completed successfully: {job_name}")
                    else:
                        print(f"Job count mismatch for {job_name}: Expected {condition}, Found {data[2]}")
                        result += 1
                else:
                    print(f"This job launched on {date_match} but did not complete successfully: {job_name}")
                    result += 1
    return result

def prepare_summary():
    """
    Main function to prepare the summary of jobs run yesterday and today.
    It checks the job statuses from the database and compares them with the expected values from CSV files.
    """
    today = datetime.date.today()
    yesterday = today - timedelta(days=1)
    weekday_str = WeekDay(yesterday.weekday()).name.capitalize()
    
    # Query to get job details from the database for yesterday
    yesterday_db_query = """
        SELECT bji.job_name, be.status, count(*) as execution_count
        FROM batch_job_execution be
        JOIN batch_job_instance bji ON bji.job_instance_id = be.job_instance_id
        WHERE be.create_time::date = current_date - 1
        GROUP BY bji.job_name, be.status;
    """
    db_yesterday_details = fetch_db_details(yesterday_db_query)
    db_batch_yesterday = [row[0] for row in db_yesterday_details]

    # Load data from yesterday's CSV file
    csv_data = load_csv_data(spath)
    
    # Categorize jobs based on CSV data
    hourly_jobs = [(row[0], row[2]) for row in csv_data if row[1] == 'YES']
    daily_jobs = [row[0] for row in csv_data if row[3] == 'YES']
    stage_day_jobs = [(row[0], row[5]) for row in csv_data if row[4] == 'YES']
    stage_week_jobs = [(row[0], row[7]) for row in csv_data if row[6] == 'YES']
    
    # Process specific day and week jobs
    specific_day_jobs = process_stage_details(stage_day_jobs)
    specific_week_jobs = process_stage_details(stage_week_jobs)

    result = 0

    # Check job statuses for yesterday
    result += check_job_status(hourly_jobs, db_yesterday_details, "Yesterday")
    result += check_job_status([(job, "") for job in daily_jobs], db_yesterday_details, "Yesterday")
    result += check_job_status([(job, time_day) for job, time_day in specific_day_jobs], db_yesterday_details, "Yesterday")
    result += check_job_status([(job, weekday_str) for job, weekday_str in specific_week_jobs], db_yesterday_details, "Yesterday")

    # Query to get job details from the database for today (between 12 AM and 6 AM)
    today_db_query = """
        SELECT bji.job_name, be.status, count(*) as execution_count
        FROM batch_job_execution be
        JOIN batch_job_instance bji ON bji.job_instance_id = be.job_instance_id
        WHERE be.create_time BETWEEN current_date AND current_date + interval '6 hours'
        GROUP BY bji.job_name, be.status;
    """
    db_today_details = fetch_db_details(today_db_query)
    db_batch_today = [row[0] for row in db_today_details]

    # Load data from today's CSV file
    csv_data_today = load_csv_data(spath2)

    # Categorize jobs based on CSV data for today
    hourly_jobs_today = [(row[0], row[2]) for row in csv_data_today if row[1] == 'YES']
    daily_jobs_today = [row[0] for row in csv_data_today if row[3] == 'YES']
    stage_day_jobs_today = [(row[0], row[5]) for row in csv_data_today if row[4] == 'YES']
    stage_week_jobs_today = [(row[0], row[7]) for row in csv_data_today if row[6] == 'YES']

    # Process specific day and week jobs for today
    specific_day_jobs_today = process_stage_details(stage_day_jobs_today)
    specific_week_jobs_today = process_stage_details(stage_week_jobs_today)

    # Check job statuses for today
    result += check_job_status(hourly_jobs_today, db_today_details, "Today")
    result += check_job_status([(job, "") for job in daily_jobs_today], db_today_details, "Today")
    result += check_job_status([(job, time_day) for job, time_day in specific_day_jobs_today], db_today_details, "Today")
    result += check_job_status([(job, weekday_str) for job, weekday_str in specific_week_jobs_today], db_today_details, "Today")

    # Print final result and exit with code 1 if any job failed
    print(result)
    if result >= 1:
        exit(1)

if __name__ == "__main__":
    # Global variables for file paths
    global spath, spath2
    spath = "batch.csv"
    # Run the summary preparation function
    prepare_summary()