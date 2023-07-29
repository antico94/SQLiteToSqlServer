import pandas as pd
from sqlalchemy import create_engine, MetaData
import pyodbc
import configparser
from colorama import Fore, Style

# Read configuration from config.ini file
config = configparser.ConfigParser()
config.read("config.ini")

# SQL Server connection settings
sql_server_server = config.get("SQL_SERVER_DATABASE", "SERVER")
sql_server_database = config.get("SQL_SERVER_DATABASE", "DATABASE")

# SQLite Connection settings
sqlite_database_file = config.get("SQLITE_DATABASE", "DATABASE_FILEPATH")
sqlite_connection_string = f"sqlite:///{sqlite_database_file}"


# Function to create the SQL Server database if it does not exist
def create_sql_server_database():
    try:
        connection = pyodbc.connect('Driver={SQL Server};'
                                    f'Server={sql_server_server};'
                                    'Trusted_Connection=yes;',
                                    autocommit=True)  # Set autocommit to True
        cursor = connection.cursor()
        cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'{sql_server_database}')"
                       f"CREATE DATABASE {sql_server_database};")
        cursor.close()
        connection.close()
        print(f"Database '{sql_server_database}' created on SQL Server.")
    except Exception as e:
        print("Error creating database on SQL Server:", e)
        raise


# Function to create tables in the SQL Server database if they do not exist
def create_sql_server_tables():
    try:
        connection = pyodbc.connect('Driver={SQL Server};'
                                    f'Server={sql_server_server};'
                                    f'Database={sql_server_database};'
                                    'Trusted_Connection=yes;')
        cursor = connection.cursor()

        # Retrieve table schema from SQLite
        engine_sqlite = create_engine(sqlite_connection_string)
        for table_name in tables_to_transfer:
            df = pd.read_sql_table(table_name, engine_sqlite)
            df_columns = df.columns.tolist()
            column_types = df.dtypes.to_dict()

            # Generate the CREATE TABLE statement for SQL Server
            create_table_query = f"CREATE TABLE {table_name} ("
            columns_info = []
            for col in df_columns:
                col_type = column_types[col]
                sql_server_type = get_sql_server_type(col_type)
                columns_info.append(f"{col} {sql_server_type}")
            create_table_query += ", ".join(columns_info)
            create_table_query += ");"

            # Check if table exists in the SQL Server database
            cursor.execute(f"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table_name}')"
                           f"{create_table_query}")
            connection.commit()
            print(f"Table '{table_name}' created on SQL Server.")

        cursor.close()
        connection.close()
    except Exception as e:
        print("Error creating tables on SQL Server:", e)
        raise


# Helper function to map SQLite data types to SQL Server data types
def get_sql_server_type(data_type):
    if data_type == "object":
        return "NVARCHAR(MAX)"
    elif data_type == "int64":
        return "BIGINT"
    elif data_type == "float64":
        return "FLOAT"
    elif data_type == "bool":
        return "BIT"
    elif data_type.startswith("datetime"):
        return "DATETIME"
    # Add more data type mappings as needed
    else:
        return "NVARCHAR(MAX)"  # Default to NVARCHAR for unknown data types


# Function to get the list of tables from SQLite database
def get_tables_from_sqlite():
    try:
        engine = create_engine(sqlite_connection_string)
        with engine.connect() as con:
            meta = MetaData()
            meta.reflect(bind=con)
            tables = list(meta.tables.keys())
        return tables
    except Exception as e:
        print("Error connecting to SQLite database:", e)
        raise


# Export data from SQLite to CSV files
def export_data_to_csv():
    engine = create_engine(sqlite_connection_string)

    for table_name in tables_to_transfer:
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, engine)
        df.to_csv(f"{table_name}.csv", index=False)

    engine.dispose()


# Import data from CSV files into SQL Server
def import_data_to_sql_server():
    try:
        connection_sql_server = pyodbc.connect('Driver={SQL Server};'
                                               f'Server={sql_server_server};'
                                               f'Database={sql_server_database};'
                                               'Trusted_Connection=yes;')
        cursor_sql_server = connection_sql_server.cursor()

        for table_name in tables_to_transfer:
            df = pd.read_csv(f"{table_name}.csv")
            df_columns = df.columns.tolist()

            # Check if table exists and has data in SQL Server
            cursor_sql_server.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL "
                                      f"AND EXISTS (SELECT 1 FROM {table_name}) "
                                      f"SELECT 1 ELSE SELECT 0;")
            table_has_data = cursor_sql_server.fetchone()[0]

            if table_has_data:
                # Truncate the table before appending new data (optional)
                cursor_sql_server.execute(f"TRUNCATE TABLE {table_name};")
                connection_sql_server.commit()

            # Prepare the INSERT INTO query
            placeholders = ','.join('?' * len(df_columns))
            insert_query = f"INSERT INTO {table_name}({','.join(df_columns)}) VALUES ({placeholders})"

            # Insert the data row by row into the SQL Server table
            for index, row in df.iterrows():
                row_values = [value if pd.notna(value) else None for value in row]
                try:
                    cursor_sql_server.execute(insert_query, tuple(row_values))
                    connection_sql_server.commit()
                    print(
                        Fore.GREEN + f"Successfully imported data from {table_name}.csv to SQL Server (Row {index + 1})." + Style.RESET_ALL)
                except Exception as insert_error:
                    print(Fore.RED + f"Error inserting data into {table_name} at Row {index + 1}.")
                    print("Row Values:", row_values)
                    print("Error:", insert_error)
                    connection_sql_server.rollback()

        cursor_sql_server.close()
        connection_sql_server.close()
    except Exception as e:
        print(Fore.RED + "Error connecting to SQL Server or importing data:", e)
        raise


if __name__ == "__main__":
    tables_to_transfer = get_tables_from_sqlite()
    print("Tables to transfer:", tables_to_transfer)

    # Create the SQL Server database and tables if they don't exist
    create_sql_server_database()
    create_sql_server_tables()

    # Perform data transfer
    export_data_to_csv()
    import_data_to_sql_server()

print("Data transfer completed!")
