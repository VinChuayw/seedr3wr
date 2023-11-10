import os
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

API_KEY = os.environ['YOUTUBE_API_KEY'] # Set your own api key in user variables for windows
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 10, 10),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG('yt_etl', default_args=default_args, schedule=None)

def extract_data():
    # Use the YouTube API to extract data (implement your logic here)
    # Example: Fetch data using the google-api-python-client library
    # Add your API key and other necessary parameters
    # See YouTube API documentation: https://developers.google.com/youtube/v3/docs
    # Example:
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    try:
        response = youtube.videos().list(part='snippet', chart='mostPopular', regionCode='US', maxResults=10).execute()
        videos = response['items',[]]
        return videos
    except HttpError as e:
        print(f'Error calling YouTube API: {e}')
        return None
    

def transform_data(videos):
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
    ])

    # Extract relevant fields from the videos list
    data = [(video['id'], video['snippet']['title'], video['snippet']['description']) for video in videos]

    # Create a DataFrame from the extracted data and schema
    spark = SparkSession.builder.appName("YouTubeETL").getOrCreate()
    df = spark.createDataFrame(data, schema=schema)
    return df

def load_data(transformed_data):
    # Load data into PostgreSQL (implement your logic here)
    connection = psycopg2.connect(user="your_user", password="your_password", host="your_host", port="your_port", database="your_database")
    cursor = connection.cursor()

    # Create a PostgreSQL table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS youtube_data (
            video_id VARCHAR PRIMARY KEY,
            title VARCHAR,
            description VARCHAR
        )
    """)

    # Insert data into the PostgreSQL table
    for row in transformed_data.collect():
        cursor.execute("""
            INSERT INTO youtube_data (video_id, title, description)
            VALUES (%s, %s, %s)
        """, (row.id, row.title, row.description))

    connection.commit()
    cursor.close()
    connection.close()

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

extract_task >> transform_task >> load_task