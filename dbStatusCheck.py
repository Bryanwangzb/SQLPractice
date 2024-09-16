import mysql.connector
import time
import sys

# Replace these with your MySQL server details
host = "restored-from-integnance-prod-db.cjkpqvmvukox.ap-northeast-1.rds.amazonaws.com"
user = "integnance"
password = "VJ0xILwcUgXkTmh"
database = "integnance"

def check_slave_status():
    try:
        # Connect to MySQL RDS
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        # Create a cursor object
        cursor = connection.cursor(dictionary=True)

        # Execute the "SHOW SLAVE STATUS;" command
        cursor.execute("SHOW SLAVE STATUS;")

        # Fetch the result
        result = cursor.fetchone()


        # Display the result
        print("Slave_IO_Running:", result["Slave_IO_Running"])
        print("Slave_SQL_Running:", result["Slave_SQL_Running"])
        print("Seconds_Behind_Master:", result["Seconds_Behind_Master"])
        print("-" * 30)

        # Check if both Slave_IO_Running and Slave_SQL_Running are "Yes"
        if result["Slave_IO_Running"] == "Yes" and result["Slave_SQL_Running"] == "Yes":
            print("Replication is running successfully. Stopping the script.")
            sys.exit()

        # Check if Slave_IO_Running or Slave_SQL_Running is "No"
        elif result["Slave_IO_Running"] == "No" or result["Slave_SQL_Running"] == "No":
            print("One of the replication threads is not running. Skipping replication error...")

            # Run "CALL mysql.rds_skip_repl_error;"
            cursor.execute("CALL mysql.rds_skip_repl_error;")

            print("Error skipped. Checking status again after 5 seconds.")

        time.sleep(10)

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()


if __name__ == "__main__":
    while True:
        check_slave_status()
