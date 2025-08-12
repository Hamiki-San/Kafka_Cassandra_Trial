import datetime

while True:

    timestamp = input("Nombor sini: ")

    if timestamp.lower() == 'done':
        print("Exit program.")
        break
    try:
        timestamp_ms = int(timestamp)
        dt_object = datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz=datetime.timezone.utc)

        print(f"Timestamp in milliseconds: {timestamp_ms}")
        print(f"Datetime object (UTC): {dt_object}")
        print(f"Human-readable (UTC): {dt_object.strftime('%Y-%m-%d %H:%M:%S.%f %Z')}")

        # If you want to see it in your local timezone (e.g., CEST for Paderborn)
        dt_local = dt_object.astimezone() # Converts to local timezone automatically
        print(f"Human-readable (Local Time - Paderborn): {dt_local.strftime('%Y-%m-%d %H:%M:%S.%f %Z')}")
        print(f"    ")

    except ValueError:
            print("Invalid input. Please enter a valid integer timestamp or 'done'.")