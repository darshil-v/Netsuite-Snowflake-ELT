from collections import OrderedDict
from datetime import datetime


def get_target_info(sf_cnxn, DATAMART_DB, DATAMART_SCHEMA):
    """
    Retrieves the target table information from the datamart.

    Args:
        sf_cnxn: The Snowflake database connection object.
        DATAMART_DB (str): The name of the datamart database.
        DATAMART_SCHEMA (str): The name of the datamart schema.

    Returns:
        tgt_tables_dict (dict): A dictionary containing the target table names as keys and empty values.

    """
    try:
        sf_cur = sf_cnxn.cursor()
        sf_cur.execute(
            f"""SELECT TABLE_NAME FROM {DATAMART_DB}.INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '{DATAMART_DB}' AND TABLE_SCHEMA = '{DATAMART_SCHEMA}' AND TABLE_NAME LIKE 'DIM_%' AND TABLE_NAME <> 'DIM_HIERARCHY';"""
        )
        tgt_tables_dict = {table[0]: "" for table in sf_cur.fetchall()}
        sf_cur.close()

        return tgt_tables_dict
    except Exception as e:
        print(e)
        return -1


def upsert_data(
    sf_cnxn,
    DATAMART_DB,
    DATAMART_SCHEMA,
    target_table,
    STAGING_DB,
    STAGING_SCHEMA,
    source_view,
    column_list,
):
    """
    Upserts data from the staging table to the target table in the datamart.

    Args:
        sf_cnxn: The Snowflake database connection object.
        DATAMART_DB (str): The name of the datamart database.
        DATAMART_SCHEMA (str): The name of the datamart schema.
        target_table (str): The name of the target table.
        STAGING_DB (str): The name of the staging database.
        STAGING_SCHEMA (str): The name of the staging schema.
        source_view (str): The name of the source view in the staging schema.
        column_list (list): A list of column names to be upserted.

    Returns:
        res: The result of the upsert operation.

    """
    try:
        sf_cur = sf_cnxn.cursor()
        merge_query = f"""
                MERGE INTO {DATAMART_DB}.{DATAMART_SCHEMA}.{target_table} AS target
                USING (
                    SELECT {', '.join([f'{col} as {col}' for col in column_list])} 
                    FROM {STAGING_DB}.{STAGING_SCHEMA}.{source_view}
                    ) AS source
                ON target.DW_KEY_ID = source.DW_KEY_ID
                WHEN MATCHED THEN
                    UPDATE SET {', '.join([f'target.{col}=source.{col}' for col in column_list])}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(column_list)}, DW_INSERT_DT)
                    VALUES ({', '.join([f'source.{col}' for col in column_list])}, '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
                """

        sf_cur.execute(merge_query)
        res = sf_cur.fetchone()
        sf_cur.close()
        return res
    except Exception as e:
        print(e)
        return -1


def get_common_columns(sf_cnxn, source, target, STAGING_DB, DATAMART_DB):
    """
    Retrieves the common columns between a source and target table.

    Args:
        sf_cnxn: The Snowflake database connection object.
        source (str): The name of the source table.
        target (str): The name of the target table.
        STAGING_DB (str): The name of the staging database.
        DATAMART_DB (str): The name of the datamart database.

    Returns:
        res (list): A list of common column names.

    """
    try:
        query = f"""
            WITH 
                S1 AS (
                  SELECT table_name, COLUMN_NAME 
                  FROM {DATAMART_DB}.INFORMATION_SCHEMA.COLUMNS 
                  WHERE table_name = '{target}'), 
                S2 AS (
                  SELECT table_name, COLUMN_NAME 
                  FROM {STAGING_DB}.INFORMATION_SCHEMA.COLUMNS 
                  WHERE table_name = '{source}') 
                SELECT S1.COLUMN_NAME DM FROM S1 INNER JOIN S2 ON S1.COLUMN_NAME = S2.COLUMN_NAME ORDER BY DM;
            """
        sf_cur = sf_cnxn.cursor()
        sf_cur.execute(query)
        res = [col[0] for col in sf_cur.fetchall()]
        sf_cur.close()
        return res
    except Exception as e:
        print(e)
        return -1


def staging_to_datamart(sf_cnxn, props, SOURCE_VIEW_KEYS):
    """
    Transfers data from staging to the datamart for the specified source view and target tables.

    Args:
        sf_cnxn: The Snowflake database connection object.
        props (dict): A dictionary containing various properties.
        SOURCE_VIEW_KEYS (dict): A dictionary containing the source view names as keys and target table names as values.

    Returns:
        int: -1 if an error occurred, otherwise None.

    """
    STAGING_DB = props["STAGING_DB"]
    STAGING_SCHEMA = props["STAGING_SCHEMA"]
    DATAMART_DB = props["DATAMART_DB"]
    DATAMART_SCHEMA = props["DATAMART_SCHEMA"]

    TARGET_TABLE_KEYS = get_target_info(sf_cnxn, DATAMART_DB, DATAMART_SCHEMA)

    if TARGET_TABLE_KEYS == -1:
        return -1

    TARGET_TABLE_KEYS = OrderedDict(sorted(TARGET_TABLE_KEYS.items()))
    SOURCE_VIEW_KEYS = OrderedDict(sorted(SOURCE_VIEW_KEYS.items()))
    SOURCE_TARGET_SET = dict(zip(TARGET_TABLE_KEYS, SOURCE_VIEW_KEYS.keys()))

    for target_table, source_view in SOURCE_TARGET_SET.items():
        try:
            column_list = get_common_columns(
                sf_cnxn, source_view, target_table, STAGING_DB, DATAMART_DB
            )
            if column_list == -1:
                print(f"{source_view}, {target_table}: Columns List Fetching Failed!!!")
                continue
            column_list.remove("DW_INSERT_DT")

            res = upsert_data(
                sf_cnxn,
                DATAMART_DB,
                DATAMART_SCHEMA,
                target_table,
                STAGING_DB,
                STAGING_SCHEMA,
                source_view,
                column_list,
            )
            if res == -1:
                print(
                    f"Upsert from {STAGING_DB}.{STAGING_SCHEMA}.{source_view} to {DATAMART_DB}.{DATAMART_SCHEMA}.{target_table} Failed!!"
                )
                continue
            else:
                print(
                    f"Upsert from {STAGING_DB}.{STAGING_SCHEMA}.{source_view} to {DATAMART_DB}.{DATAMART_SCHEMA}.{target_table} Completed!!"
                )
                print(f"{res[0]} rows Inserted and {res[1]} rows Updated")
        except Exception as e:
            print(
                f"{STAGING_DB}.{STAGING_SCHEMA}.{source_view}, {DATAMART_DB}.{DATAMART_SCHEMA}.{target_table}:",
                e,
            )
            continue
