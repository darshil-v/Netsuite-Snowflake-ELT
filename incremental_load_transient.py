# import pandas as pd
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas
from ns_to_sf_transform import transform_data
from tables import getPrimaryKeyTables

PRIMARY_KEY_TABLES = getPrimaryKeyTables()

def check_date_last_modified(df, env, table_name):
    """
    Check the last modified date for a table in the control table DataFrame.

    Args:
        df: Control table DataFrame
        env (str): The environment of the table.
        table_name (str): The name of the table.

    Returns:
        str: The last modified date if found in the control table.
             Returns "1970-01-01 00:00:00" if no records are found.
             Returns -1 if an error occurs during the execution.
    """
    try:
        # Assuming your DataFrame is called 'df'
        last_modified_date = df.loc[
            (df["ENV"] == f"{env}") & ((df["NETSUITE_TABLE_NAME"]).str.upper() == f"{table_name}"),
            "LAST_MODIFIED_DATE",
        ].max()

        if last_modified_date:
            return last_modified_date
        else:
            print(f"No records in NetSuite-to-Landing control table for {table_name}!")
            # Return default value
            return "1970-01-01 00:00:00"
    except Exception as e:
        print(table_name, ":", e)
        return -1


def fetch_data_ns(ns_cnxn, table, last_modified_date):
    """
    Fetch data from a table in NetSuite based on the last modified date.

    Args:
        ns_cnxn: The NetSuite database connection.
        table (str): The name of the table to fetch data from.
        last_modified_date (str): The last modified date to filter the data.

    Returns:
        tuple: A tuple containing two elements:
            - A list of column names.
            - A list of fetched data rows.
        Returns (-1, -1) if an error occurs during the execution.
    """
    try:
        query = (
            f"SELECT * FROM {table} WHERE DATE_LAST_MODIFIED > '{last_modified_date}'"
        )

        with ns_cnxn.cursor() as ns_cursor:
            ns_cursor.execute(query)
            columns = [desc[0] for desc in ns_cursor.description]
            data = ns_cursor.fetchall()
        return columns, data
    except Exception as e:
        print(table, ":", e)
        return -1, -1


def update_control_table(
    sf_cnxn, env, ns_table_name, incr_modified_date, control_table
):
    """
    Update the control table with the last modified date for a table.

    Args:
        sf_cnxn: The Snowflake database connection.
        env (str): The environment of the table.
        ns_table_name (str): The name of the table in NetSuite.
        incr_modified_date (str): The incremental modified date to update in the control table.
        control_table (str): The name of the control table.

    Returns:
        None
    """
    try:
        ct_scd_query = (
            f"MERGE INTO {control_table} t USING (SELECT '{env}' AS env, '{ns_table_name}' AS table_name, '{incr_modified_date}' AS last_modified_date) s "
            f"ON (t.ENV = s.env AND t.NETSUITE_TABLE_NAME ILIKE s.table_name) "
            f"WHEN MATCHED THEN UPDATE SET t.last_modified_date = s.last_modified_date "
            f"WHEN NOT MATCHED THEN INSERT (ENV, NETSUITE_TABLE_NAME, last_modified_date) "
            f"VALUES (s.env, s.table_name, s.last_modified_date)"
        )
        with sf_cnxn.cursor() as sf_cur:
            sf_cur.execute(ct_scd_query)

        print(f"{ns_table_name}: Control table updated on {incr_modified_date}")
    except Exception as e:
        print(ns_table_name, ":", e)
        return -1


def fetch_control_table(sf_cnxn, control_table_name):
    """
    Fetches control table and loads it into a DataFrame.

    Args:
        sf_cnxn: The Snowflake database connection.
        control_table_name (str): The name of the control table.

    Returns:
        DataFrame: Control table DataFrame.
    """
    try:
        ct_fetch_query = f"SELECT * FROM {control_table_name};"

        with sf_cnxn.cursor() as sf_cur:
            sf_cur.execute(ct_fetch_query)
            return sf_cur.fetch_pandas_all()
    except Exception as e:
        print("Control table error:", e)

def merge_snowflake(sf_cnxn, sf_data, table, landing_db, landing_schema, transient_schema):
    columns = sf_data.columns

    merge_query = (f"""
            MERGE INTO {landing_db}.{landing_schema}.{table} TGT USING {landing_db}.{transient_schema}.{table} SRC
            ON TGT.{PRIMARY_KEY_TABLES[table]} = SRC.{PRIMARY_KEY_TABLES[table]}
            WHEN MATCHED THEN UPDATE SET {', '.join([f'TGT.{col} = SRC.{col}' for col in columns])} 
            WHEN NOT MATCHED THEN INSERT 
            VALUES ({', '.join([f'SRC.{col}' for col in columns])});
            """
        )

    with sf_cnxn.cursor() as sf_cur:
        sf_cur.execute(merge_query)
        print(sf_cur.fetchone(), "values upserted to Landing!")

def incremental_load_transient(ns_cnxn, sf_cnxn, KEY_TABLES, PRIMARY_KEY_TABLES, props):
    """
    Load tables from NetSuite to Snowflake incrementally for specific interval.

    Args:
        ns_cnxn: The NetSuite database connection.
        sf_cnxn: The Snowflake database connection.
        KEY_TABLES (list): A list of table names to bulk load.
        PRIMARY_KEY_TABLES (dict): A dictionary containing primary key information for each table.
        props (dict): A dictionary of additional properties.

    Returns:
        None
    """
    LANDING_DB = props["LANDING_DB"]
    LANDING_SCHEMA = props["LANDING_SCHEMA"]
    ENV = props["ENV"]
    TRANSIENT_SCHEMA = "FINANCE_TRANSIENT"
    CONTROL_TABLE = props["CONTROL_TABLE"]

    control_table_df = fetch_control_table(sf_cnxn, control_table_name=CONTROL_TABLE)

    for table in KEY_TABLES:
        try:
            ct_last_mod_dt = check_date_last_modified(
                df=control_table_df, env=ENV, table_name=table
            )
            if ct_last_mod_dt == -1:
                continue

            columns, data = fetch_data_ns(ns_cnxn, table, ct_last_mod_dt)

            if columns == -1 or data == -1:
                print(f"Fetching {table} data from NetSuite Failed!!!")
                continue

            print(
                f"\n{table}: Data collected from NetSuite. Uploading to Snowflake.."
            )
            df = transform_data(data, columns, table, PRIMARY_KEY_TABLES)
            # print(df)
            with sf_cnxn.cursor() as sf_cur:
                sf_cur.execute(
                    f"TRUNCATE TABLE {LANDING_DB}.{TRANSIENT_SCHEMA}.{table}"
                )
            print(f"{table}: Transient table truncated")

            write_pandas(
                conn=sf_cnxn,
                df=df,
                table_name=table,
                quote_identifiers=False,
                database=LANDING_DB,
                schema=TRANSIENT_SCHEMA,
            )
            print(f"{table}: Snowflake Transient table data loaded!")

            merge_snowflake(sf_cnxn, sf_data=df, table=table, landing_db=LANDING_DB, landing_schema=LANDING_SCHEMA, transient_schema=TRANSIENT_SCHEMA)
            print(f"{table}: Snowflake Landing table data loaded!")

            update_control_table(
                sf_cnxn,
                env=ENV,
                ns_table_name=table,
                incr_modified_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                # source_df['DATE_LAST_MODIFIED'].values.max())
                control_table=props["CONTROL_TABLE"],
            )

        except Exception as e:
            print(f"ERROR in {table}: {e}")
            continue
        finally:
            sf_cnxn.commit()
