import time
import logging
from datetime import datetime, timezone
import pytz
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Import your configuration module
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

def create_new_table(session):
    """Interactively creates a new table in the keyspace."""
    print("\n--- Create New Table ---")
    new_table_name = input("Enter the name for the new table: ").strip()
    if not new_table_name:
        print("❌ Table name cannot be empty.")
        return None
    
    new_data_column = input("Enter the data column name (e.g., 'payload'): ").strip()
    if not new_data_column:
        print("❌ Data column name cannot be empty.")
        return None
        
    cql = f"""
    CREATE TABLE {new_table_name} (
        sensor_id text,
        event_created timestamp,
        {new_data_column} text,
        PRIMARY KEY (sensor_id, event_created)
    );
    """
    try:
        session.execute(cql)
        print(f"✅ Successfully created table '{new_table_name}' with data column '{new_data_column}'.")
        return {
            'table': new_table_name,
            'sensor_id_for_cassandra': new_table_name,
            'data_column': new_data_column
        }
    except Exception as e:
        logging.error(f"❌ Failed to create table: {e}")
        return None

def query_mode(session, topic_to_table_mapping):
    """Handles the query mode logic."""
    print("\n--- Query Existing Data ---")
    while True:
        date_str = input("Enter the date (YYYY-MM-DD): ").strip()
        from_time_str = input("Enter the start time (HH:MM:SS): ").strip()
        until_time_str = input("Enter the end time (HH:MM:SS): ").strip()

        try:
            start_datetime_str = f"{date_str} {from_time_str}"
            end_datetime_str = f"{date_str} {until_time_str}"
            
            start_ts = datetime.strptime(start_datetime_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            end_ts = datetime.strptime(end_datetime_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        except ValueError:
            print("❌ Error: Invalid date or time format. Please use YYYY-MM-DD and HH:MM:SS.")
            try_again = input("Do you want to try again? (y/n): ").strip().lower()
            if try_again not in ['y', 'yes']:
                return # Exit query mode
            continue

        print(f"\nSearching all tables for data between {start_ts.isoformat()} and {end_ts.isoformat()}...")

        found_data = False
        for topic_name, config in topic_to_table_mapping.items():
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
                    print(f"  - Timestamp: {row.event_created.isoformat()}, URI: {row.payload}")
            else:
                print("⚠️ No data found in this time range.")
        
        if not found_data:
            print("\n🚫 No data was found across all tables for the specified time range.")
        
        print("-" * 50)
        
        another_query = input("\nDo you want to perform another query? (y/n): ").strip().lower()
        if another_query not in ['y', 'yes']:
            break

def main():
    """Main function to handle interactive menu."""
    session = get_cassandra_session()

    if not session:
        print("Cannot proceed without a Cassandra connection.")
        return

    # Use a mutable copy of the mapping for the current session
    topic_to_table_mapping = consumer_config.TOPIC_TO_TABLE_MAPPING.copy()

    print("\n--- Cassandra Interactive Tool ---")
    
    while True:
        print("\nChoose an action:")
        print("1. Query existing tables")
        print("2. Create a new table")
        print("3. Exit")
        choice = input("Enter your choice (1, 2, or 3): ").strip()
        
        if choice == '1':
            query_mode(session, topic_to_table_mapping)
        elif choice == '2':
            new_table_info = create_new_table(session)
            if new_table_info:
                # Add the new table to the mapping for the current session
                topic_to_table_mapping[new_table_info['table']] = new_table_info
        elif choice == '3':
            print("Exiting tool.")
            break
        else:
            print("❌ Invalid choice. Please enter 1, 2, or 3.")

    if session:
        session.shutdown()
        logging.info("Cassandra session closed.")

if __name__ == "__main__":
    main()