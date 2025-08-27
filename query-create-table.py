import time
import logging
from datetime import datetime, timezone
import pytz
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import connection
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

def get_cassandra_cluster():
    """Establishes and returns a Cassandra cluster object."""
    try:
        auth_provider = PlainTextAuthProvider(username=consumer_config.CASSANDRA_USERNAME, password=consumer_config.CASSANDRA_PASSWORD)
        cluster = Cluster(consumer_config.CASSANDRA_HOSTS, port=consumer_config.CASSANDRA_PORT, auth_provider=auth_provider)
        logging.info("🟢 Successfully connected to Cassandra cluster.")
        return cluster
    except Exception as e:
        logging.error(f"❌ Failed to connect to Cassandra cluster: {e}")
        return None

def manage_keyspaces(cluster):
    """Allows user to list, create, or switch keyspaces."""
    while True:
        print("\n--- Keyspace Management ---")
        print("1. List existing keyspaces")
        print("2. Create a new keyspace")
        print("3. Choose an existing keyspace")
        print("4. Back to main menu")
        choice = input("Enter your choice: ").strip()

        if choice == '1':
            try:
                session = cluster.connect()
                keyspaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
                print("\nExisting Keyspaces:")
                for row in keyspaces:
                    print(f"- {row.keyspace_name}")
                session.shutdown()
            except Exception as e:
                logging.error(f"❌ Failed to list keyspaces: {e}")
        
        elif choice == '2':
            new_ks_name = input("Enter the name for the new keyspace: ").strip()
            if not new_ks_name:
                print("❌ Keyspace name cannot be empty.")
                continue
            
            replication_strategy = input("Enter replication strategy (e.g., SimpleStrategy): ").strip() or "SimpleStrategy"
            replication_factor = input(f"Enter replication factor for {replication_strategy} (e.g., 1): ").strip() or "1"
            
            cql = f"""
            CREATE KEYSPACE IF NOT EXISTS {new_ks_name}
            WITH replication = {{'class': '{replication_strategy}', 'replication_factor': '{replication_factor}'}}
            AND durable_writes = true;
            """
            try:
                session = cluster.connect()
                session.execute(cql)
                print(f"✅ Keyspace '{new_ks_name}' created successfully.")
                session.shutdown()
            except Exception as e:
                logging.error(f"❌ Failed to create keyspace: {e}")

        elif choice == '3':
            keyspace_name = input("Enter the keyspace name to use: ").strip()
            if not keyspace_name:
                print("❌ Keyspace name cannot be empty.")
                continue
            
            try:
                session = cluster.connect(keyspace_name)
                print(f"✅ Successfully switched to keyspace '{keyspace_name}'.")
                return session, keyspace_name
            except Exception as e:
                logging.error(f"❌ Failed to connect to keyspace '{keyspace_name}': {e}")
        
        elif choice == '4':
            return None, None
        
        else:
            print("❌ Invalid choice. Please enter a valid option.")
    
def query_data_by_time_range(
    session,
    keyspace_name: str,
    table_name: str,
    partition_key_column: str,
    partition_key_value: str,
    start_time: datetime,
    end_time: datetime
):
    """
    Queries data from a specified Cassandra table for a given time range.
    """
    if not session:
        return None

    try:
        query_cql = f"""
        SELECT *
        FROM {keyspace_name}.{table_name}
        WHERE {partition_key_column} = ?
          AND event_created > ?
          AND event_created < ?
        ALLOW FILTERING;
        """
        prepared_stmt = session.prepare(query_cql)
        rows = session.execute(prepared_stmt, (partition_key_value, start_time, end_time))
        return list(rows)
    except Exception as e:
        logging.error(f"❌ Failed to execute query for '{keyspace_name}.{table_name}': {e}")
        return None

def create_new_table(session, keyspace_name: str):
    """Interactively creates a new table with a user-defined partition key."""
    if not session or not keyspace_name:
        print("🚫 Please select a keyspace first.")
        return None
        
    print(f"\n--- Create New Table in '{keyspace_name}' ---")
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
    CREATE TABLE {keyspace_name}.{new_table_name} (
        {partition_key_col} text,
        event_created timestamp,
        {new_data_column} text,
        PRIMARY KEY ({partition_key_col}, event_created)
    );
    """
    try:
        session.execute(cql)
        print(f"✅ Successfully created table '{new_table_name}' in keyspace '{keyspace_name}'.")
        return {
            'table': new_table_name,
            'partition_key': partition_key_col,
            'data_column': new_data_column
        }
    except Exception as e:
        logging.error(f"❌ Failed to create table: {e}")
        return None

def drop_table(session, keyspace_name: str):
    """Interactively drops a table from the keyspace with confirmation."""
    if not session or not keyspace_name:
        print("🚫 Please select a keyspace first.")
        return
        
    print(f"\n--- Drop a Table from '{keyspace_name}' ---")
    table_to_drop = input("Enter the name of the table to drop: ").strip()
    if not table_to_drop:
        print("❌ Table name cannot be empty.")
        return

    confirm = input(f"Are you sure you want to drop the table '{table_to_drop}' from keyspace '{keyspace_name}'? This action cannot be undone. (y/n): ").strip().lower()
    if confirm in ['y', 'yes']:
        cql = f"DROP TABLE {keyspace_name}.{table_to_drop};"
        try:
            session.execute(cql)
            print(f"✅ Successfully dropped table '{table_to_drop}'.")
        except Exception as e:
            logging.error(f"❌ Failed to drop table: {e}")
    else:
        print("Drop operation cancelled.")

def migrate_data(session, keyspace_name: str):
    """Migrates all data from one table to another, with user-defined partition keys."""
    if not session or not keyspace_name:
        print("🚫 Please select a keyspace first.")
        return
    
    print(f"\n--- Migrate Data in '{keyspace_name}' ---")
    source_table = input("Enter the name of the source table (the one to migrate from): ").strip()
    dest_table = input("Enter the name of the destination table (the one to migrate to): ").strip()
    
    if not source_table or not dest_table:
        print("❌ Source and destination table names cannot be empty.")
        return

    source_pk_col = input(f"Enter the partition key column name for '{source_table}' (e.g., 'sensor_id'): ").strip()
    dest_pk_col = input(f"Enter the partition key column name for '{dest_table}' (e.g., 'topic'): ").strip()
    
    if not source_pk_col or not dest_pk_col:
        print("❌ Partition key column names cannot be empty.")
        return

    # 1. Read data from the source table
    print(f"Reading data from '{source_table}'...")
    try:
        source_rows = session.execute(f"SELECT * FROM {keyspace_name}.{source_table}")
    except Exception as e:
        logging.error(f"❌ Failed to read from source table '{source_table}': {e}")
        return

    # 2. Get column names for both tables
    try:
        dest_cols = [row.column_name for row in session.execute(f"SELECT * FROM system_schema.columns WHERE keyspace_name = '{keyspace_name}' AND table_name = '{dest_table}'")]
    except Exception as e:
        logging.error(f"❌ Failed to retrieve destination table schema: {e}")
        return

    # 3. Prepare the insert statement for the destination table
    insert_cql = f"INSERT INTO {keyspace_name}.{dest_table} ({', '.join(dest_cols)}) VALUES ({', '.join(['?' for _ in dest_cols])});"
    prepared_insert = session.prepare(insert_cql)

    # 4. Insert data into the destination table
    print(f"Migrating data to '{dest_table}'...")
    migrated_count = 0
    try:
        for row in source_rows:
            new_row_values = {}
            for col in dest_cols:
                if col == dest_pk_col:
                    new_row_values[col] = getattr(row, source_pk_col)
                elif hasattr(row, col):
                    new_row_values[col] = getattr(row, col)
                else:
                    new_row_values[col] = None 
            
            session.execute(prepared_insert, [new_row_values[col] for col in dest_cols])
            migrated_count += 1
        
        print(f"✅ Successfully migrated {migrated_count} rows from '{source_table}' to '{dest_table}'.")

        drop_after_migrate = input(f"Do you want to drop the source table '{source_table}' now? (y/n): ").strip().lower()
        if drop_after_migrate in ['y', 'yes']:
            drop_table(session, keyspace_name)

    except Exception as e:
        logging.error(f"❌ Failed to migrate data: {e}")
        print(f"Migration aborted after {migrated_count} rows.")


def query_mode(session, keyspace_name: str):
    """Handles the query mode logic."""
    if not session or not keyspace_name:
        print("🚫 Please select a keyspace first.")
        return
        
    print(f"\n--- Query Existing Data from '{keyspace_name}' ---")
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
            keyspace_name=keyspace_name,
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
    cluster = get_cassandra_cluster()
    if not cluster:
        print("Cannot proceed without a Cassandra connection.")
        return

    session = None
    keyspace = None

    print("\n--- Cassandra Interactive Tool ---")
    
    while True:
        status = f"Connected to cluster. Keyspace: {keyspace or 'None'}"
        print(f"\n{status}")
        print("\nChoose an action:")
        print("1. Manage keyspaces (list, create, choose)")
        print("2. Query existing data from a table")
        print("3. Create a new table")
        print("4. Migrate data from one table to another")
        print("5. Drop a table")
        print("6. Exit")
        choice = input("Enter your choice (1-6): ").strip()
        
        if choice == '1':
            session, keyspace = manage_keyspaces(cluster)
        elif choice == '2':
            query_mode(session, keyspace)
        elif choice == '3':
            create_new_table(session, keyspace)
        elif choice == '4':
            migrate_data(session, keyspace)
        elif choice == '5':
            drop_table(session, keyspace)
        elif choice == '6':
            print("Exiting tool.")
            break
        else:
            print("❌ Invalid choice. Please enter a valid option.")

    if session:
        session.shutdown()
        logging.info("Cassandra session closed.")
    if cluster:
        cluster.shutdown()
        logging.info("Cassandra cluster closed.")

if __name__ == "__main__":
    main()