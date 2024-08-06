import sys
import mysql.connector
import configparser
import time
import os
import hashlib
import shutil

config = configparser.ConfigParser()
config.sections()

config.read("config.ini")

db_host = config["LOGIN"]["host"]
db_user = config["LOGIN"]["user"]
db_passwd = config["LOGIN"]["password"]
db_database = config["LOGIN"]["database"]
folder_original = config["FOLDERS"]["folder_source"]
folder_backup = config["FOLDERS"]["folder_backup"]

def connect_to_db():
    try:
        database = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_passwd,
        )
    except Exception as e:
        print("Error:", e)
        return False, e
    else:
        print("Database Connection Successful.")
        return database, False


def check_database_names():
    cursor = database.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_database};")
    cursor.execute(f"USE {db_database}")
    cursor.execute(f"DROP TABLE IF EXISTS source_checksums")
    database.commit()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS source_checksums (
            ID INT AUTO_INCREMENT PRIMARY KEY,
            filepath TEXT,
            checksum TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS backup_checksums (
            ID INT AUTO_INCREMENT PRIMARY KEY,
            filepath TEXT,
            checksum TEXT
        )
    """)
    cursor.close()


def database_add(filepath, checksum, position):
    cursor = database.cursor()
    if position == "source":
        cursor.execute(f"SELECT checksum FROM source_checksums WHERE filepath = %s", (filepath,))
        result = cursor.fetchone()
        if result is None:
            cursor.execute(f"INSERT INTO source_checksums (filepath, checksum) VALUES (%s, %s)",
                           (filepath, checksum))
        else:
            existing_checksum = result[0]
            if existing_checksum != checksum:
                cursor.execute(f"UPDATE source_checksums SET checksum = %s WHERE filepath = %s",
                               (checksum, filepath))
        database.commit()
    if position == "backup":
        cursor.execute(f"SELECT checksum FROM backup_checksums WHERE filepath = %s", (filepath,))
        result = cursor.fetchone()
        if result is None:
            cursor.execute(f"INSERT INTO backup_checksums (filepath, checksum) VALUES (%s, %s)",
                           (filepath, checksum))
        else:
            existing_checksum = result[0]
            if existing_checksum != checksum:
                cursor.execute(f"UPDATE backup_checksums SET checksum = %s WHERE filepath = %s",
                               (checksum, filepath))
    database.commit()
    cursor.close()


# Find all files in directory and send each file to generate_checksum
def process_directory(directory, folder_num):
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            # print(f"Processing file: {file_path}")  # Debug print
            checksum = generate_checksum(file_path)
            if checksum:
                # Define if using original or backup folder
                if folder_num == 0:
                    # Remove the folder location so that they can be compared
                    file_path = file_path.removeprefix(folder_original)
                    # Add file path and checksum to db
                    database_add(file_path, checksum, "source")

                elif folder_num == 1:
                    # Remove the folder location so that they can be compared
                    file_path = file_path.removeprefix(folder_backup)
                    # Add file path and checksum to db
                    database_add(file_path, checksum, "backup")


def check_backup_checksums():
    cursor = database.cursor()
    try:
        cursor.execute("SELECT checksum FROM backup_checksums")
        result = cursor.fetchone()
        # Fetchall to prevent unfetched data
        cursor.fetchall()
    finally:
        cursor.close()

    if result is None:
        return False
    else:
        return True


def generate_checksum(file_path):
    sha256 = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            for block in iter(lambda: f.read(4096), b''):
                sha256.update(block)
    except IOError:
        print(f"Could not read file: {file_path}. Check Permissions.")
        return None
    return sha256.hexdigest()


def pull_database(table):
    cursor = database.cursor()
    try:
        cursor.execute(f"SELECT filepath, checksum FROM {table}")
        result = cursor.fetchall()
        return result
    finally:
        cursor.close()

def delete_file(filepath):
    try:
        print("Deleting:", filepath)
        os.remove(filepath)
        return True
    except Exception as e:
        print("Error when deleting old backup file:", e)
        return False

def copy(item):
    try:
        shutil.copy(folder_original + item, folder_backup + item)
        return True
    except FileNotFoundError as e:
        print("Directory not found making.")
        directory = os.path.dirname(folder_backup + item)
        os.makedirs(directory, exist_ok=True)
        # Do it again
        shutil.copy(folder_original + item, folder_backup + item)
    except PermissionError as e:
        print(e)
    except Exception as e:
        print(e)


def copy_file(filepath):
    print("Copying file:", folder_original + filepath, "to", folder_backup + filepath)
    # Check if old backup exists
    if os.path.isfile(folder_backup + filepath):
        # Check permissions on backup location
        if os.access(folder_backup + filepath, os.W_OK):
            # Attempt to delete file
            if delete_file(folder_backup + filepath):
                # File deleted so copy new file
                print("Successfully deleted:", folder_backup + filepath, "Copying new.")
                copy(filepath)
            else:
                print("Failed to delete file", folder_backup + filepath, "Skipping.")
                return False
    else:
        copy(filepath)


def check_against(source_array, backup_array):
    match = 0
    for source in source_array:
        for backup in backup_array:
            if source == backup:
                match = 1
                break
        if match == 1:
            match = 0
        else:
            # Send filepath to copy_file function
            copy_file(source[0])


def main():
    check_database_names() # Check if database / tables exist, if not create

    if not os.path.exists(folder_original):
        print(f"Original folder does not exist: {folder_original}")
        sys.exit(0)
    if not os.path.exists(folder_backup):
        print(f"Backup folder does not exist: {folder_backup}")
        try:
            print(f"Attempting to create {folder_backup}")
            os.makedirs(folder_backup, exist_ok=True)
        except Exception as e:
            print("Failed to create directory:", e)
            print("Closing...")
            time.sleep(2)
            sys.exit(0)
    print("Successfully created backup directory. Proceeding...")

    print("\nGenerating Checksum for", folder_original)
    process_directory(folder_original, 0)

    backup_checksums_exist = check_backup_checksums()

    if backup_checksums_exist is False:
        print("\nGenerating Checksum for", folder_backup)
        process_directory(folder_backup, 1)

    source_array = pull_database("source_checksums")
    backup_array = pull_database("backup_checksums")
    check_against(source_array, backup_array)


database, db_error = connect_to_db()


if database:
    main()
else:
    print("Program exiting. Could not connect to database with error:", db_error)
    time.sleep(2)
    sys.exit(0)
