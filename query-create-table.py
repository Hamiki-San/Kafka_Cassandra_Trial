import time
import logging
from datetime import datetime, timezone
import pytz
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel

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
    partition_key_column: str,
    partition_key_value: str,
    start_time: datetime,
    end_time: datetime
):
    """
    Queries data from a specified Cassandra table for a given time range.

    Args:
        session (cassandra.cluster.Session): The active Cassandra session.
        table_name (str): The name of the table to query.
        partition_key_column (str): The name of the partition key column (e.g., 'sensor_id', 'topic').
        partition_key_value (str): The value of the partition key for the query.
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
        WHERE {partition_key_column} = ?
          AND event_created > ?
          AND event_created < ?
        ALLOW FILTERING;
        """
        prepared_stmt = session.prepare(query_cql)
        rows = session.execute(prepared_stmt, (partition_key_value, start_time, end_time))
        return list(rows)
    except Exception as e:
        logging.error(f"❌ Failed to execute query for '{table_name}': {e}")
        return None

def create_new_table(session):
    """Interactively creates a new table with a user-defined partition key."""
    print("\n--- Create New Table ---")
    new_table_name = input("Enter the name for the new table: ").strip()
    if not new_table_name:
        print("❌ Table name cannot be empty.")
        return None

    partition_key_col = input("Enter the name for the partition key column (e.g., 'topic'): ").strip()
    if not partition_key_col:
        print("❌ Partition key column name cannot be empty.")
        return None

    new_data_column = input("Enter the data column name (e.g., 'payload'): ").strip()
    if new_data_column.lower() in [partition_key_col.lower(), 'event_created']:
        print("❌ Error: The data column name cannot be the same as the partition key or 'event_created'.")
        return None

    cql = f"""
    CREATE TABLE {new_table_name} (
        {partition_key_col} text,
        event_created timestamp,
        {new_data_column} text,
        PRIMARY KEY ({partition_key_col}, event_created)
    );
    """
    try:
        session.execute(cql)
        print(f"✅ Successfully created table '{new_table_name}' with data column '{new_data_column}'.")
        return {
            'table': new_table_name,
            'partition_key': partition_key_col,
            'data_column': new_data_column
        }
    except Exception as e:
        logging.error(f"❌ Failed to create table: {e}")
        return None

def drop_table(session):
    """Interactively drops a table from the keyspace with confirmation."""
    print("\n--- Drop a Table ---")
    table_to_drop = input("Enter the name of the table to drop: ").strip()
    if not table_to_drop:
        print("❌ Table name cannot be empty.")
        return

    confirm = input(f"Are you sure you want to drop the table '{table_to_drop}'? This action cannot be undone. (y/n): ").strip().lower()
    if confirm in ['y', 'yes']:
        cql = f"DROP TABLE {table_to_drop};"
        try:
            session.execute(cql)
            print(f"✅ Successfully dropped table '{table_to_drop}'.")
        except Exception as e:
            logging.error(f"❌ Failed to drop table: {e}")
    else:
        print("Drop operation cancelled.")

def migrate_data(session):
    """Migrates all data from one table to another, with user-defined partition keys."""
    print("\n--- Migrate Data from One Table to Another ---")
    source_table = input("Enter the name of the source table (the one to migrate from): ").strip()
    dest_table = input("Enter the name of the destination table (the one to migrate to): ").strip()
    
    if not source_table or not dest_table:
        print("❌ Source and destination table names cannot be empty.")
        return

    # Prompt for the partition key names to enable correct mapping
    source_pk_col = input(f"Enter the partition key column name for '{source_table}' (e.g., 'sensor_id'): ").strip()
    dest_pk_col = input(f"Enter the partition key column name for '{dest_table}' (e.g., 'topic'): ").strip()
    
    if not source_pk_col or not dest_pk_col:
        print("❌ Partition key column names cannot be empty.")
        return

    # 1. Read data from the source table
    print(f"Reading data from '{source_table}'...")
    try:
        source_rows = session.execute(f"SELECT * FROM {source_table}")
    except Exception as e:
        logging.error(f"❌ Failed to read from source table '{source_table}': {e}")
        return

    # 2. Get column names for both tables
    try:
        dest_cols = [row.column_name for row in session.execute(f"SELECT * FROM system_schema.columns WHERE keyspace_name = '{consumer_config.KEYSPACE_NAME}' AND table_name = '{dest_table}'")]
    except Exception as e:
        logging.error(f"❌ Failed to retrieve destination table schema: {e}")
        return

    # 3. Prepare the insert statement for the destination table
    insert_cql = f"INSERT INTO {dest_table} ({', '.join(dest_cols)}) VALUES ({', '.join(['?' for _ in dest_cols])});"
    prepared_insert = session.prepare(insert_cql)

    # 4. Insert data into the destination table
    print(f"Migrating data to '{dest_table}'...")
    migrated_count = 0
    try:
        for row in source_rows:
            # Create a dictionary to hold the new row's values
            new_row_values = {}
            for col in dest_cols:
                # Explicitly map the source partition key to the destination partition key
                if col == dest_pk_col:
                    new_row_values[col] = getattr(row, source_pk_col)
                elif hasattr(row, col):
                    new_row_values[col] = getattr(row, col)
                else:
                    # Handle cases where a column exists in the destination but not the source (e.g., a new column)
                    new_row_values[col] = None 
            
            # Execute the insert
            session.execute(prepared_insert, [new_row_values[col] for col in dest_cols])
            migrated_count += 1
        
        print(f"✅ Successfully migrated {migrated_count} rows from '{source_table}' to '{dest_table}'.")

        # Optional: Drop the source table after successful migration
        drop_after_migrate = input(f"Do you want to drop the source table '{source_table}' now? (y/n): ").strip().lower()
        if drop_after_migrate in ['y', 'yes']:
            drop_table(session)

    except Exception as e:
        logging.error(f"❌ Failed to migrate data: {e}")
        print(f"Migration aborted after {migrated_count} rows.")


def query_mode(session):
    """Handles the query mode logic."""
    print("\n--- Query Existing Data ---")
    while True:
        table_name = input("Enter the table name to query: ").strip()
        partition_key_col = input("Enter the partition key column name (e.g., 'sensor_id' or 'topic'): ").strip()
        partition_key_val = input("Enter the partition key value: ").strip()

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
            try_again = input("Do you want to continue query? (y/n): ").strip().lower()
            if try_again not in ['y', 'yes']:
                return
            continue

        print(f"\nSearching table '{table_name}' for data with {partition_key_col} = '{partition_key_val}' between {start_ts.isoformat()} and {end_ts.isoformat()}...")

        data_rows = query_data_by_time_range(
            session=session,
            table_name=table_name,
            partition_key_column=partition_key_col,
            partition_key_value=partition_key_val,
            start_time=start_ts,
            end_time=end_ts
        )

        if data_rows:
            print(f"✅ Found {len(data_rows)} rows:")
            for row in data_rows:
                print(f"  - Timestamp: {row.event_created.isoformat()}, Data: {row.payload}")
        else:
            print("⚠️ No data found in this time range.")
        
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

    print("\n--- Cassandra Interactive Tool ---")
    
    while True:
        print("\nChoose an action:")
        print("1. Query existing data from a table")
        print("2. Create a new table")
        print("3. Migrate data from one table to another")
        print("4. Drop a table")
        print("5. Exit")
        choice = input("Enter your choice (1, 2, 3, 4, or 5): ").strip()
        
        if choice == '1':
            query_mode(session)
        elif choice == '2':
            create_new_table(session)
        elif choice == '3':
            migrate_data(session)
        elif choice == '4':
            drop_table(session)
        elif choice == '5':
            print("Exiting tool.")
            break
        else:
            print("❌ Invalid choice. Please enter 1, 2, 3, 4, or 5.")

    if session:
        session.shutdown()
        logging.info("Cassandra session closed.")

if __name__ == "__main__":
    main()