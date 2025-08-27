import time
import logging
from datetime import datetime, timezone
import pytz
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Import your configuration module, this is where this module know which keysapce to look at
try:
    import consumer_config
except ImportError:
    print("Error: The 'consumer_config.py' file was not found.")
    print("Please make sure it's in the same directory.")
    exit()

# Configure logging for better feedback
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_cassandra_session():
    """Establishes and returns a Cassandra session."""
    try:
        auth_provider = PlainTextAuthProvider(username=consumer_config.CASSANDRA_USERNAME, password=consumer_config.CASSANDRA_PASSWORD)
        cluster = Cluster(consumer_config.CASSANDRA_HOSTS, port=consumer_config.CASSANDRA_PORT, auth_provider=auth_provider)
        session = cluster.connect(consumer_config.KEYSPACE_NAME)
        logging.info("🟢 Successfully connected to Cassandra.")
        return session
    except Exception as e:
        logging.error(f"❌ Failed to connect to Cassandra: {e}")
        return None

def query_data_by_time_range(
    session, 
    table_name: str, 
    sensor_id: str, 
    start_time: datetime, 
    end_time: datetime
):
    """
    Queries data from a specified Cassandra table for a given time range.
    
    Args:
        session (cassandra.cluster.Session): The active Cassandra session.
        table_name (str): The name of the table to query.
        sensor_id (str): The sensor ID for the partition key.
        start_time (datetime): The start timestamp for the query range.
        end_time (datetime): The end timestamp for the query range.

    Returns:
        list: A list of result rows from the query, or None on failure.
    """
    if not session:
        return None

    try:
        query_cql = f"""
        SELECT *
        FROM {table_name}
        WHERE sensor_id = ?
          AND event_created > ?
          AND event_created < ?
        ALLOW FILTERING;
        """
        prepared_stmt = session.prepare(query_cql)
        rows = session.execute(prepared_stmt, (sensor_id, start_time, end_time))
        return list(rows)
    except Exception as e:
        logging.error(f"❌ Failed to execute query for '{table_name}': {e}")
        return None

def main():
    """Main function to handle interactive queries across all topics."""
    session = get_cassandra_session()

    if not session:
        print("Cannot proceed without a Cassandra connection.")
        return

    print("\n--- Automated Cassandra Query Tool ---")
    
    while True: # Main loop for continuous querying
        try:
            date_str = input("Enter the date (YYYY-MM-DD): ").strip()
            from_time_str = input("Enter the start time (HH:MM:SS): ").strip()
            until_time_str = input("Enter the end time (HH:MM:SS): ").strip()

            try:
                start_datetime_str = f"{date_str} {from_time_str}"
                end_datetime_str = f"{date_str} {until_time_str}"
                
                # Convert user input strings to datetime objects
                start_ts = datetime.strptime(start_datetime_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                end_ts = datetime.strptime(end_datetime_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            except ValueError:
                print("❌ Error: Invalid date or time format. Please use YYYY-MM-DD and HH:MM:SS.")
                try_again = input("Do you want to continue query? (y/n): ").strip().lower()
                if try_again not in ['y', 'yes']:
                    break # Exit the main loop
                else:
                    continue # Go to the top of the loop to re-enter dates

            print(f"\nSearching all tables for data between {start_ts.isoformat()} and {end_ts.isoformat()}...")

            found_data = False
            for topic_name, config in consumer_config.TOPIC_TO_TABLE_MAPPING.items():
                table_name = config['table']
                sensor_id = config['sensor_id_for_cassandra']
                
                print(f"\n--- Checking table: '{table_name}' ---")
                
                data_rows = query_data_by_time_range(
                    session=session,
                    table_name=table_name,
                    sensor_id=sensor_id,
                    start_time=start_ts,
                    end_time=end_ts
                )

                if data_rows:
                    found_data = True
                    print(f"✅ Found {len(data_rows)} rows:")
                    for row in data_rows:
                        # 'payload' is the column that stores the MinIO URI
                        print(f"  - Timestamp: {row.event_created.isoformat()}, URI: {row.payload}")
                else:
                    print("⚠️ No data found in this time range.")
            
            if not found_data:
                print("\n🚫 No data was found across all tables for the specified time range.")
            
            print("-" * 50)
            
            # Ask the user if they want to perform another query
            another_query = input("\nDo you want to perform another query? (y/n): ").strip().lower()
            if another_query not in ['y', 'yes']:
                break # Exit the main loop if input is not 'y' or 'yes'

        except KeyboardInterrupt:
            print("\nExiting query tool.")
            break
            
    if session:
        session.shutdown()
        logging.info("Cassandra session closed.")

if __name__ == "__main__":
    main()