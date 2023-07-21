def ns_query(table, ns_cnxn):
    """
    Execute a query to fetch column information for a table in NetSuite.

    Args:
        table (str): The name of the table to query.
        ns_cnxn: The NetSuite database connection.

    Returns:
        list: A list of tuples containing the fetched column information.
    """
    query = f"select table_name,column_name,type_name,oa_length,oa_precision,oa_scale from oa_columns where table_name='{table}'"
    # print(query)
    with ns_cnxn.cursor() as ns_cursor:
        ns_cursor.execute(query)
        data = ns_cursor.fetchall()
    return data


def get_ddl_query(rows, LANDING_DB, LANDING_SCHEMA, PRIMARY_KEY_TABLES, SF_DATATYPES):
    """
    Generate a SQL statement for creating or replacing a table in Snowflake.

    Args:
        rows (list): The fetched column information for a table.
        LANDING_DB (str): The name of the Snowflake landing database.
        LANDING_SCHEMA (str): The name of the Snowflake landing schema.
        PRIMARY_KEY_TABLES (dict): A dictionary containing primary key information for each table.
        SF_DATATYPES (dict): A dictionary mapping data types.

    Returns:
        str: The SQL statement for creating or replacing a table in Snowflake.
    """
    tableName = rows[0][0]
    header = f"""CREATE OR REPLACE TABLE {LANDING_DB}.{LANDING_SCHEMA}.{tableName}(\n
        """
    sql_statement = ""
    for row in rows:
        # matching datatypes with length and precision
        dtype = f"{SF_DATATYPES[row[2]]}"
        if dtype == "NUMBER":
            # Checking if precision less than 37 for dtype NUMBER
            if row[4] > 37:
                dtype = "NUMBER(38,0)"
            else:
                dtype += f"({row[4]},{row[5]})"
        elif dtype == "VARCHAR":
            dtype += f"({row[4]})"
        sql_statement += f"{row[1]} {dtype},\n"
    sql_statement = (
        header + sql_statement + f"PRIMARY KEY ({PRIMARY_KEY_TABLES[tableName]})" + ");"
    )
    return sql_statement


def sf_query(sf_cnxn, query):
    """
    Execute a query in Snowflake and fetch the result.

    Args:
        sf_cnxn: The Snowflake database connection.
        query (str): The SQL query to execute.

    Returns:
        object: The fetched data if successful, or -1 if an error occurs.
    """
    try:
        with sf_cnxn.cursor() as sf_cursor:
            sf_cursor.execute(query)
            data = sf_cursor.fetchall()
    except Exception as e:
        print(e)
        return -1

    return data


def load_tables(ns_cnxn, sf_cnxn, KEY_TABLES, PRIMARY_KEY_TABLES, SF_DATATYPES, props):
    """
    Load tables from NetSuite to Snowflake.

    Args:
        ns_cnxn: The NetSuite database connection.
        sf_cnxn: The Snowflake database connection.
        KEY_TABLES (list): A list of table names to load.
        PRIMARY_KEY_TABLES (dict): A dictionary containing primary key information for each table.
        SF_DATATYPES (dict): A dictionary mapping data types.
        props (dict): A dictionary of additional properties.

    Returns:
        None
    """
    LANDING_DB = props["LANDING_DB"]
    LANDING_SCHEMA = props["LANDING_SCHEMA"]

    for table in KEY_TABLES:
        ns_query_res = ns_query(table, ns_cnxn)
        if ns_query_res:
            query = get_ddl_query(
                ns_query_res,
                LANDING_DB,
                LANDING_SCHEMA,
                PRIMARY_KEY_TABLES,
                SF_DATATYPES,
            )
            sf_query_res = sf_query(sf_cnxn, query)
            if sf_query_res != -1:
                print(sf_query_res)
            else:
                print("Snowflake: Table creation failed")
        else:
            print(f"NetSuite: Table - {table} fetching failed")

    sf_cnxn.commit()
