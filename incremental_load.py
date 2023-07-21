# import pandas as pd
from datetime import *
from ns_to_sf_transform import transform_data


def check_date_last_modified(sf_cnxn, control_table_name, env, table_name):
    """
    Check the last modified date for a table in the control table.

    Args:
        sf_cnxn: The Snowflake database connection.
        control_table_name (str): The name of the control table.
        env (str): The environment of the table.
        table_name (str): The name of the table.

    Returns:
        str: The last modified date if found in the control table.
             Returns "1970-01-01 00:00:00" if no records are found.
             Returns -1 if an error occurs during the execution.
    """
    try:
        ct_fetch_query = f"SELECT LAST_MODIFIED_DATE FROM {control_table_name} WHERE ENV='{env}' AND NETSUITE_TABLE_NAME ILIKE '{table_name}';"

        with sf_cnxn.cursor() as sf_cur:
            sf_cur.execute(ct_fetch_query)
            last_modified_date = sf_cur.fetchone()[0]

        if last_modified_date:
            return last_modified_date
        else:
            print(f"No records in {control_table_name} for {table_name}!")
            return "1970-01-01 00:00:00"
    except Exception as e:
        print(table_name, ":", e)
        return -1


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
        sf_cur = sf_cnxn.cursor()
        sf_cur.execute(ct_scd_query)
        sf_cnxn.commit()

        print(f"{ns_table_name}: Control table updated on {incr_modified_date}")
    except Exception as e:
        print(ns_table_name, ":", e)
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


def upsert_to_snowflake(sf_cnxn, sf_data, table, id_cols):
    """
    Upsert data from a Pandas DataFrame to Snowflake.

    Args:
        sf_cnxn: The Snowflake database connection.
        sf_data (DataFrame): The Pandas DataFrame containing the data to upsert.
        table (str): The name of the table in Snowflake.
        id_cols (list): A list of column names used as identifiers for upsert.

    Returns:
        bool: True if the upsert operation is successful, False otherwise.
    """
    try:
        columns = sf_data.columns
        column_placeholders = ",".join(["%s"] * len(columns))

        upsert_query = (
            f"MERGE INTO {table} AS target USING (VALUES ({column_placeholders})) AS source({','.join([col for col in columns])}) "
            f"ON ({' AND '.join([f'target.{col} = source.{col}' for col in id_cols])}) "
            f"WHEN MATCHED THEN UPDATE SET {','.join([f'target.{col} = source.{col}' for col in columns])} "
            f"WHEN NOT MATCHED THEN INSERT ({','.join([col for col in columns])}) "
            f"VALUES ({','.join([f'source.{col}' for col in columns])});"
        )

        with sf_cnxn.cursor() as sf_cur:
            sf_cur.executemany(upsert_query, sf_data.values.tolist())
            res = sf_cur.fetchall()
            sf_cur.close()

        if res[0][0] == 1:
            print(f"{table}: Delta records upsert successful!!!")
            return True
        else:
            print(f"{table}: Delta records upsert failed!!!")
            return False
    except Exception as e:
        print(f"{table}: Upsert Failed!!! - {e}")
        return False


def incremental_load(ns_cnxn, sf_cnxn, KEY_TABLES, PRIMARY_KEY_TABLES, props):
    """
    Perform an incremental data load from NetSuite to Snowflake.

    Args:
        ns_cnxn: The NetSuite database connection.
        sf_cnxn: The Snowflake database connection.
        KEY_TABLES (list): A list of table names to be processed incrementally.
        PRIMARY_KEY_TABLES (dict): A dictionary mapping table names to their primary key column names.
        props (dict): A dictionary of properties containing relevant configuration values.

    Returns:
        None

    Raises:
        Exception: If an error occurs during the execution.

    """
    CONTROL_TABLE = props["CONTROL_TABLE"]
    ENV = props["ENV"]
    LANDING_DB = props["LANDING_DB"]
    LANDING_SCHEMA = props["LANDING_SCHEMA"]

    for table in KEY_TABLES:
        try:
            with sf_cnxn.cursor() as sf_cur:
                sf_cur.execute(f"USE {LANDING_DB}.{LANDING_SCHEMA};")
                sf_cur.close()

            # get watermarked (LAST_MODIFIED_DATE) column from control table
            ct_dt = check_date_last_modified(sf_cnxn, CONTROL_TABLE, ENV, table)
            if ct_dt == -1:
                continue

            # filter the records from NetSuite based on the watermarked (LAST_MODIFIED_DATE) column
            columns, data = fetch_data_ns(ns_cnxn, table, ct_dt)
            if columns == -1 or data == -1:
                continue

            if len(data) == 0:
                print(table, ": No new records to upsert")
                continue

            sf_data = transform_data(data, columns, table, PRIMARY_KEY_TABLES)
            id_cols = [PRIMARY_KEY_TABLES[table]]

            # upsert the newly processed records to snowflake
            upsertRes = upsert_to_snowflake(sf_cnxn, sf_data, table, id_cols)

            if upsertRes is False:
                continue

            # update the control table
            upsert_ct_res = update_control_table(
                sf_cnxn,
                env="INFOFISCUS_PYTHON_LANDING",
                ns_table_name=table,
                incr_modified_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                # source_df['DATE_LAST_MODIFIED'].values.max())
                control_table=CONTROL_TABLE,
            )
            if upsert_ct_res is False:
                continue

        except UnicodeDecodeError as ude:
            print(table, ":", ude)
            continue
        except Exception as e:
            print(table, ":", e)
            continue
    print("Incremental Upload Completed")
