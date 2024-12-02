from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
import pymysql
import boto3
import logging
import time


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='/logs/spotify_etl.log'
)
logger = logging.getLogger('spotify_etl')

# DAG configuration
dag = DAG(
    'spotify_etl',
    description='ETL para proyecto Spotify',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

def get_mysql_credentials():
    conn = BaseHook.get_connection("mysql_credentials")
    mysql_credentials = {
        "host": conn.host,
        "login": conn.login,
        "password": conn.password,
        "port": conn.port
    }
    return mysql_credentials


def get_aws_credentials():
    conn = BaseHook.get_connection("aws_credentials")
    aws_credentials = {
        "access_key": conn.login,
        "secret_access_key": conn.password,
        "session_token": conn.extra_dejson.get("aws_session_token"),
    }
    return aws_credentials

# Athena configuration
ATHENA_CONFIG = {
    'region_name': 'us-east-1',
    'schema_name': 'spotify_db',
    's3_staging_dir': 's3://spotify-project-athena'
}

@task(dag=dag)
def extract_top_artists():
    logger.info("Starting top artists extraction")


    aws_credentials = get_aws_credentials()

    athena_client = boto3.client(
        "athena",
        aws_access_key_id=aws_credentials["access_key"],
        aws_secret_access_key=aws_credentials["secret_access_key"],
        aws_session_token=aws_credentials["session_token"],
        region_name=ATHENA_CONFIG['region_name']
    )
        
    query = """
    SELECT 
        artist_name,
        AVG(TRY_CAST(REGEXP_REPLACE(popularity, '[^0-9.]', '') AS DOUBLE)) as avg_popularity,
        COUNT(DISTINCT song_title) as total_songs,
        COUNT(DISTINCT album_name) as total_albums
    FROM songs
    GROUP BY artist_name
    ORDER BY avg_popularity DESC
    """
    query_response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_CONFIG['schema_name']},
        ResultConfiguration={
            "OutputLocation": ATHENA_CONFIG['s3_staging_dir'],
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    query_execution_id = query_response["QueryExecutionId"]

    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = query_status['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
            
        time.sleep(1) 
    
    if state == 'SUCCEEDED':
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        for row in results['ResultSet']['Rows'][1:]:
            rows.append([field.get('VarCharValue') for field in row['Data']])
        
        df = pd.DataFrame(rows, columns=columns)

        df.to_csv('/tmp/top_artists.csv', index=False)
        logger.info(f"Extracted {len(df)} top artists")
        return '/tmp/top_artists.csv'
    else:
        error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        logger.error(f"Query failed: {error_message}")
        raise Exception(f"Athena query failed: {error_message}")


@task(dag=dag)
def extract_user_listening_patterns():
    logger.info("Starting user listening patterns extraction")
    
    aws_credentials = get_aws_credentials()

    athena_client = boto3.client(
        "athena",
        aws_access_key_id=aws_credentials["access_key"],
        aws_secret_access_key=aws_credentials["secret_access_key"],
        aws_session_token=aws_credentials["session_token"],
        region_name=ATHENA_CONFIG['region_name']
    )
    
    query = """
    SELECT 
        ur.user_email,
        COUNT(*) as total_plays,
        COUNT(DISTINCT ur.song_title) as unique_songs,
        AVG(TRY_CAST(REGEXP_REPLACE(ur.replay_duration, '[^0-9.]', '') AS DOUBLE)) as avg_duration
    FROM user_replays ur
    GROUP BY ur.user_email
    """
    
    query_response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_CONFIG['schema_name']},
        ResultConfiguration={
            "OutputLocation": ATHENA_CONFIG['s3_staging_dir'],
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    query_execution_id = query_response["QueryExecutionId"]

    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = query_status['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
            
        time.sleep(1) 
    
    if state == 'SUCCEEDED':
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        for row in results['ResultSet']['Rows'][1:]:
            rows.append([field.get('VarCharValue') for field in row['Data']])
        
        df = pd.DataFrame(rows, columns=columns)
        
        df.to_csv('/tmp/user_patterns.csv', index=False)
        logger.info(f"Extracted {len(df)} user listening patterns")
        return '/tmp/user_patterns.csv'
    else:
        error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        logger.error(f"Query failed: {error_message}")
        raise Exception(f"Athena query failed: {error_message}")
    

@task(dag=dag)
def extract_playlist_analytics():
    logger.info("Starting playlist analytics extraction")
    
    aws_credentials = get_aws_credentials()

    athena_client = boto3.client(
        "athena",
        aws_access_key_id=aws_credentials["access_key"],
        aws_secret_access_key=aws_credentials["secret_access_key"],
        aws_session_token=aws_credentials["session_token"],
        region_name=ATHENA_CONFIG['region_name']
    )
    
    query = """
    SELECT 
        p.user_email,
        COUNT(DISTINCT p.playlist_name) as total_playlists,
        AVG(TRY_CAST(REGEXP_REPLACE(p.songs_number, '[^0-9.]', '') AS DOUBLE)) as avg_songs_per_playlist,
        SUM(TRY_CAST(REGEXP_REPLACE(p.total_time, '[^0-9.]', '') AS DOUBLE)) as total_playlist_duration
    FROM playlists p
    GROUP BY p.user_email
    """
    
    query_response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_CONFIG['schema_name']},
        ResultConfiguration={
            "OutputLocation": ATHENA_CONFIG['s3_staging_dir'],
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    query_execution_id = query_response["QueryExecutionId"]

    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = query_status['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
            
        time.sleep(1) 
    
    if state == 'SUCCEEDED':
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        for row in results['ResultSet']['Rows'][1:]:
            rows.append([field.get('VarCharValue') for field in row['Data']])
        
        df = pd.DataFrame(rows, columns=columns)
        
        df.to_csv('/tmp/playlist_analytics.csv', index=False)
        logger.info(f"Extracted {len(df)} playlist analytics records")
        return '/tmp/playlist_analytics.csv'
    else:
        error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        logger.error(f"Query failed: {error_message}")
        raise Exception(f"Athena query failed: {error_message}")
    
@task(dag=dag)
def extract_genre_popularity():
    logger.info("Starting genre popularity extraction")
    
    aws_credentials = get_aws_credentials()

    athena_client = boto3.client(
        "athena",
        aws_access_key_id=aws_credentials["access_key"],
        aws_secret_access_key=aws_credentials["secret_access_key"],
        aws_session_token=aws_credentials["session_token"],
        region_name=ATHENA_CONFIG['region_name']
    )
    
    query = """
    SELECT 
        a.genres as genre,
        COUNT(DISTINCT s.song_title) as total_songs,
        COUNT(DISTINCT a.artist_name) as total_artists,
        AVG(TRY_CAST(REGEXP_REPLACE(s.popularity, '[^0-9.]', '') AS DOUBLE)) as avg_song_popularity,
        AVG(TRY_CAST(REGEXP_REPLACE(a.popularity, '[^0-9.]', '') AS DOUBLE)) as avg_artist_popularity
    FROM artists a
    JOIN songs s ON a.artist_name = s.artist_name
    GROUP BY a.genres
    """
    
    query_response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_CONFIG['schema_name']},
        ResultConfiguration={
            "OutputLocation": ATHENA_CONFIG['s3_staging_dir'],
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    query_execution_id = query_response["QueryExecutionId"]

    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = query_status['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
            
        time.sleep(1) 
    
    if state == 'SUCCEEDED':
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        for row in results['ResultSet']['Rows'][1:]:
            rows.append([field.get('VarCharValue') for field in row['Data']])
        
        df = pd.DataFrame(rows, columns=columns)
        
        df.to_csv('/tmp/genre_popularity.csv', index=False)
        logger.info(f"Extracted {len(df)} genre popularity records")
        return '/tmp/genre_popularity.csv'
    else:
        error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        logger.error(f"Query failed: {error_message}")
        raise Exception(f"Athena query failed: {error_message}")

@task(dag=dag)
def extract_user_favorite_genres():
    logger.info("Starting user favorite genres extraction")
    
    aws_credentials = get_aws_credentials()

    athena_client = boto3.client(
        "athena",
        aws_access_key_id=aws_credentials["access_key"],
        aws_secret_access_key=aws_credentials["secret_access_key"],
        aws_session_token=aws_credentials["session_token"],
        region_name=ATHENA_CONFIG['region_name']
    )
    
    query = """
    SELECT 
        ur.user_email,
        a.genres as genre,
        COUNT(*) as play_count,
        AVG(TRY_CAST(REGEXP_REPLACE(ur.replay_duration, '[^0-9.]', '') AS DOUBLE)) as avg_listen_duration
    FROM user_replays ur
    JOIN songs s ON ur.song_title = s.song_title
    JOIN artists a ON s.artist_name = a.artist_name
    GROUP BY ur.user_email, a.genres
    """
    
    query_response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_CONFIG['schema_name']},
        ResultConfiguration={
            "OutputLocation": ATHENA_CONFIG['s3_staging_dir'],
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )

    query_execution_id = query_response["QueryExecutionId"]

    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = query_status['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
            
        time.sleep(1) 
    
    if state == 'SUCCEEDED':
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        for row in results['ResultSet']['Rows'][1:]:
            rows.append([field.get('VarCharValue') for field in row['Data']])
        
        df = pd.DataFrame(rows, columns=columns)
        
        df.to_csv('/tmp/user_favorite_genres.csv', index=False)
        logger.info(f"Extracted {len(df)} user favorite genres records")
        return '/tmp/user_favorite_genres.csv'
    else:
        error_message = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        logger.error(f"Query failed: {error_message}")
        raise Exception(f"Athena query failed: {error_message}")


@task(dag=dag)
def transform_and_load():
    logger.info("Starting transform and load process")
    
    mysql_credentials = get_mysql_credentials()
    conn = pymysql.connect(
        host=mysql_credentials["host"],
        user=mysql_credentials["login"],
        password=mysql_credentials["password"],
        port=mysql_credentials["port"]
    )
    cursor = conn.cursor()
    
    try:
        # Create summary tables if they don't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS spotify.artist_summary (
            artist_name VARCHAR(255) PRIMARY KEY,
            popularity_score FLOAT,
            total_songs INT,
            total_albums INT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS spotify.user_listening_summary (
            user_email VARCHAR(255) PRIMARY KEY,
            total_plays INT,
            unique_songs INT,
            avg_duration FLOAT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS spotify.playlist_summary (
            user_email VARCHAR(255) PRIMARY KEY,
            total_playlists INT,
            avg_songs_per_playlist FLOAT,
            total_duration INT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS spotify.genre_popularity_summary (
            genre VARCHAR(255) PRIMARY KEY,
            total_songs INT,
            total_artists INT,
            avg_song_popularity FLOAT,
            avg_artist_popularity FLOAT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS spotify.user_favorite_genres_summary (
            user_email VARCHAR(255),
            genre VARCHAR(255),
            play_count INT,
            avg_listen_duration FLOAT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_email, genre),
            FOREIGN KEY (user_email) REFERENCES user_listening_summary(user_email),
            FOREIGN KEY (genre) REFERENCES genre_popularity_summary(genre)
        )
        """)
        
        # Load transformed data
        # Artists
        df_artists = pd.read_csv('/tmp/top_artists.csv')
        df_artists = df_artists.fillna(0)
        for _, row in df_artists.iterrows():
            cursor.execute("""
            INSERT INTO spotify.artist_summary (artist_name, popularity_score, total_songs, total_albums)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                popularity_score = VALUES(popularity_score),
                total_songs = VALUES(total_songs),
                total_albums = VALUES(total_albums),
                last_updated = CURRENT_TIMESTAMP
            """, (row['artist_name'], row['avg_popularity'], row['total_songs'], row['total_albums']))
        
        # User Listening Patterns
        df_users = pd.read_csv('/tmp/user_patterns.csv')
        df_users = df_users.fillna(0)
        for _, row in df_users.iterrows():
            cursor.execute("""
            INSERT INTO spotify.user_listening_summary (user_email, total_plays, unique_songs, avg_duration)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_plays = VALUES(total_plays),
                unique_songs = VALUES(unique_songs),
                avg_duration = VALUES(avg_duration),
                last_updated = CURRENT_TIMESTAMP
            """, (row['user_email'], row['total_plays'], row['unique_songs'], row['avg_duration']))
        
        # Playlist Analytics
        df_playlists = pd.read_csv('/tmp/playlist_analytics.csv')
        df_playlists = df_playlists.fillna(0)
        for _, row in df_playlists.iterrows():
            cursor.execute("""
            INSERT INTO spotify.playlist_summary (user_email, total_playlists, avg_songs_per_playlist, total_duration)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_playlists = VALUES(total_playlists),
                avg_songs_per_playlist = VALUES(avg_songs_per_playlist),
                total_duration = VALUES(total_duration),
                last_updated = CURRENT_TIMESTAMP
            """, (row['user_email'], row['total_playlists'], row['avg_songs_per_playlist'], row['total_playlist_duration']))
        
        # Genre Popularity
        df_genres = pd.read_csv('/tmp/genre_popularity.csv')
        df_genres = df_genres.fillna(0)
        for _, row in df_genres.iterrows():
            cursor.execute("""
            INSERT INTO spotify.genre_popularity_summary (genre, total_songs, total_artists, avg_song_popularity, avg_artist_popularity)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_songs = VALUES(total_songs),
                total_artists = VALUES(total_artists),
                avg_song_popularity = VALUES(avg_song_popularity),
                avg_artist_popularity = VALUES(avg_artist_popularity),
                last_updated = CURRENT_TIMESTAMP
            """, (row['genre'], row['total_songs'], row['total_artists'], row['avg_song_popularity'], row['avg_artist_popularity']))

        # User Favorite Genres
        df_user_genres = pd.read_csv('/tmp/user_favorite_genres.csv')
        df_user_genres = df_user_genres.fillna(0)
        for _, row in df_user_genres.iterrows():
            cursor.execute("""
            INSERT INTO spotify.user_favorite_genres_summary (user_email, genre, play_count, avg_listen_duration)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                play_count = VALUES(play_count),
                avg_listen_duration = VALUES(avg_listen_duration),
                last_updated = CURRENT_TIMESTAMP
            """, (row['user_email'], row['genre'], row['play_count'], row['avg_listen_duration']))
        
        conn.commit()
        logger.info("Successfully loaded all data into MySQL")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error in transform_and_load: {str(e)}")
        raise
    
    finally:
        cursor.close()
        conn.close()

# Set up task dependencies
top_artists = extract_top_artists()
user_patterns = extract_user_listening_patterns()
playlist_analytics = extract_playlist_analytics()
genre_popularity = extract_genre_popularity()
user_favorite_genres = extract_user_favorite_genres()
load_task = transform_and_load()

[top_artists, user_patterns, playlist_analytics, genre_popularity, user_favorite_genres] >> load_task