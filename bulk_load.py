# import pandas as pd
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas
from ns_to_sf_transform import transform_data


def fetch_data_ns(ns_cnxn, table):
    """
    Fetch data from a table in NetSuite.

    Args:
        ns_cnxn: The NetSuite database connection.
        table (str): The name of the table to fetch data from.

    Returns:
        tuple: A tuple containing two elements:
            - A list of column names.
            - A list of fetched data rows.
            Returns (-1, -1) if an error occurs during the execution.
    """
    query = f"SELECT * FROM {table};"
    try:
        with ns_cnxn.cursor() as ns_cursor:
            ns_cursor.execute(query)
            columns = [desc[0] for desc in ns_cursor.description]
            data = ns_cursor.fetchall()
        return columns, data
    except Exception as e:
        print(e)
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
        sf_cur = sf_cnxn.cursor()
        sf_cur.execute(ct_scd_query)
        sf_cnxn.commit()

        print(f"{ns_table_name}: Control table updated on {incr_modified_date}")
    except Exception as e:
        print(ns_table_name, ":", e)
        return -1


def bulk_load(ns_cnxn, sf_cnxn, KEY_TABLES, PRIMARY_KEY_TABLES, props):
    """
    Bulk load tables from NetSuite to Snowflake.

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
    TRANSIENT_SCHEMA = "FINANCE_TRANSIENT"

    for table in KEY_TABLES:
        try:
            columns, data = fetch_data_ns(ns_cnxn, table)

            if columns == -1 or data == -1:
                print(f"Fetching {table} data from NetSuite Failed!!!")
                continue

            print(
                f"Data collected for {table} from NetSuite. Bulk Uploading to Snowflake.."
            )
            df = transform_data(data, columns, table, PRIMARY_KEY_TABLES)
            # print(df)
            with sf_cnxn.cursor() as sf_cur:
                sf_cur.execute(
                    f"TRUNCATE TABLE {LANDING_DB}.{TRANSIENT_SCHEMA}.{table}"
                )

            write_pandas(
                conn=sf_cnxn,
                df=df,
                table_name=table,
                quote_identifiers=False,
                database=LANDING_DB,
                schema=TRANSIENT_SCHEMA,
            )
            print(f"{table}: Bulk Uploading to Snowflake Complete!")

            update_control_table(
                sf_cnxn,
                env="INFOFISCUS_PYTHON_LANDING",
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
            sf_cnxn.close()
