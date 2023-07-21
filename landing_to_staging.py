from datetime import datetime
import snowflake.connector as sc
import pandas as pd


def get_control_table(sf_conn, CONTROL_TABLE):
    """
    Retrieve the data from the control table in Snowflake.

    Args:
        sf_conn: The Snowflake database connection.
        CONTROL_TABLE (str): The name of the control table.

    Returns:
        DataFrame: The data from the control table as a Pandas DataFrame.

    """
    with sf_conn.cursor() as sf_cur:
        sf_cur.execute(f"SELECT * FROM {CONTROL_TABLE}")
        ct_data = sf_cur.fetch_pandas_all()
        sf_cur.close()
    return ct_data


def update_control_table(
    sf_conn,
    table,
    last_mod_ts,
    ct_data,
    SRC_VIEW_TABLE,
    CONTROL_TABLE,
    STAGING_DB,
    STAGING_SCHEMA,
    LANDING_DB,
    LANDING_SCHEMA,
):
    """
    Update the control table in Snowflake with the latest run information.

    Args:
        sf_conn: The Snowflake database connection.
        table (tuple): The table metadata (name, target database, target schema).
        last_mod_ts (str): The last modified timestamp for the table.
        ct_data (DataFrame): The data from the control table.
        SRC_VIEW_TABLE (dict): A dictionary mapping source table names to their corresponding view names.
        CONTROL_TABLE (str): The name of the control table.
        STAGING_DB (str): The name of the staging database.
        STAGING_SCHEMA (str): The name of the staging schema.
        LANDING_DB (str): The name of the landing database.
        LANDING_SCHEMA (str): The name of the landing schema.

    Returns:
        None

    """
    ct_scd_query = (
        f"MERGE INTO {CONTROL_TABLE} t USING (SELECT '{STAGING_DB}' AS TGT_DB, '{STAGING_SCHEMA}' AS TGT_SCHEMA, '{table[1]}' AS TGT_TABLE, '{last_mod_ts}' AS LAST_RUN_DATE_TIME) s "
        f"ON (t.TGT_DB = s.TGT_DB AND t.TGT_SCHEMA = s.TGT_SCHEMA AND t.TGT_TABLE = s.TGT_TABLE) "
        f"WHEN MATCHED THEN UPDATE SET t.LAST_RUN_DATE_TIME = '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}' "
        f"WHEN NOT MATCHED THEN INSERT (ROW_NUM, SRC_DB, SRC_SCHEMA, SRC_TABLE, SRC_VIEW, TGT_DB, TGT_SCHEMA, TGT_TABLE, LAST_RUN_DATE_TIME) "
        f"VALUES ({ct_data['ROW_NUM'].values.max() + 1}, '{LANDING_DB}', '{LANDING_SCHEMA}', '{SRC_VIEW_TABLE[table[0]]}', '{table[0]}', s.TGT_DB, s.TGT_SCHEMA, s.TGT_TABLE, '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}')"
    )

    with sf_conn.cursor() as sf_cur:
        sf_cur.execute(ct_scd_query)
        sf_cur.close()

    print(f"{CONTROL_TABLE.split('.')[-1]} updated for {table[1]}")


def check_new_records(sf_conn, table, last_mod_ts, LANDING_DB, LANDING_SCHEMA):
    """
    Check if there are new records in the landing table since the last modified timestamp.

    Args:
        sf_conn: The Snowflake database connection.
        table (tuple): The table metadata (name, target database, target schema).
        last_mod_ts (str): The last modified timestamp for the table.
        LANDING_DB (str): The name of the landing database.
        LANDING_SCHEMA (str): The name of the landing schema.

    Returns:
        int: The count of new records.

    """
    with sf_conn.cursor() as sf_cur:
        sf_cur.execute(
            f"""SELECT COUNT(*) FROM {LANDING_DB}.{LANDING_SCHEMA}.{table[0]} lv WHERE lv.INSERT_DT > '{last_mod_ts}' """
        )
        res = sf_cur.fetchone()[0]
        sf_cur.close()
    return res


def insert_to_snowflake(
    sf_conn, table, last_mod_ts, STAGING_DB, STAGING_SCHEMA, LANDING_DB, LANDING_SCHEMA
):
    """
    Truncate the staging table and insert new records from the landing table to Snowflake.

    Args:
        sf_conn: The Snowflake database connection.
        table (tuple): The table metadata (name, target database, target schema).
        last_mod_ts (str): The last modified timestamp for the table.
        STAGING_DB (str): The name of the staging database.
        STAGING_SCHEMA (str): The name of the staging schema.
        LANDING_DB (str): The name of the landing database.
        LANDING_SCHEMA (str): The name of the landing schema.

    Returns:
        None

    """
    sf_cur = sf_conn.cursor()
    # Truncate Staging table before Insert
    truncate_query = f"""TRUNCATE TABLE {STAGING_DB}.{STAGING_SCHEMA}.{table[1]}"""
    sf_cur.execute(truncate_query)

    # Insert data into tables
    insert_query = f"""
    INSERT INTO {STAGING_DB}.{STAGING_SCHEMA}.{table[1]} (SELECT * FROM {LANDING_DB}.{LANDING_SCHEMA}.{table[0]} lv WHERE lv.INSERT_DT > '{last_mod_ts}')
    """
    sf_cur.execute(insert_query)
    print(f"\n{table[1]} truncated and loaded...")


def landing_to_staging(sf_conn, props):
    """
    Perform the landing-to-staging process for the specified tables.

    Args:
        sf_conn: The Snowflake database connection.
        props (dict): A dictionary containing the properties/configuration for the landing-to-staging process.

    Returns:
        None

    """
    CONTROL_TABLE = props["CONTROL_TABLE"]
    LANDING_DB = props["LANDING_DB"]
    LANDING_SCHEMA = props["LANDING_SCHEMA"]
    STAGING_DB = props["STAGING_DB"]
    STAGING_SCHEMA = props["STAGING_SCHEMA"]

    ct_data = get_control_table(sf_conn, CONTROL_TABLE)
    SRC_VIEW_TABLE = dict(zip(ct_data.SRC_VIEW.values, ct_data.SRC_TABLE.values))
    KEY_TABLES = list(zip(ct_data.SRC_VIEW.values, ct_data.TGT_TABLE.values))

    for table in KEY_TABLES:
        last_modified_dt = pd.to_datetime(
            str(
                ct_data[
                    (ct_data["SRC_VIEW"] == table[0])
                    & (ct_data["TGT_TABLE"] == table[1])
                ]["LAST_RUN_DATE_TIME"].values[0]
            )
        )
        last_modified_dt = last_modified_dt.strftime("%Y-%m-%d %H:%M:%S")

        try:
            num_records = check_new_records(
                sf_conn, table, last_modified_dt, LANDING_DB, LANDING_SCHEMA
            )
            if num_records > 0:
                insert_to_snowflake(
                    sf_conn,
                    table,
                    last_modified_dt,
                    STAGING_DB,
                    STAGING_SCHEMA,
                    LANDING_DB,
                    LANDING_SCHEMA,
                )
                update_control_table(
                    sf_conn,
                    table,
                    last_modified_dt,
                    ct_data,
                    SRC_VIEW_TABLE,
                    CONTROL_TABLE,
                    STAGING_DB,
                    STAGING_SCHEMA,
                    LANDING_DB,
                    LANDING_SCHEMA,
                )
                sf_conn.commit()
            else:
                print(
                    f"No new records for {table[1]} in {LANDING_DB}.{LANDING_SCHEMA}\n"
                )
        except KeyError as ke:
            print(
                f"\nError with {table[0]}: Check if user has access privilege and/or object exists!"
            )
            continue
        except sc.errors.ProgrammingError as pe:
            print(pe)
            continue
