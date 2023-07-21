import pandas as pd


def transform_data(data, columns, table, PRIMARY_KEY_TABLES):
    """
    Transform the fetched data from NetSuite before loading it into Snowflake.

    Args:
        data (list): The fetched data rows from NetSuite.
        columns (list): The column names of the fetched data.
        table (str): The name of the table being processed.
        PRIMARY_KEY_TABLES (dict): A dictionary mapping table names to their primary key column names.

    Returns:
        DataFrame: The transformed data as a Pandas DataFrame.

    """
    data = [list(each) for each in data]
    df = pd.DataFrame(data=data, columns=columns)
    df[PRIMARY_KEY_TABLES[table]] = df[PRIMARY_KEY_TABLES[table]].astype(int)

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col].fillna("1970-01-01 00:00:00", inplace=True)
            df[col] = pd.to_datetime(df[col])
            df[col] = df[col].astype(str)
        elif pd.api.types.is_int64_dtype(df[col]):
            df[col].fillna(0, inplace=True)
        elif pd.api.types.is_float_dtype(df[col]):
            df[col].fillna(0.0, inplace=True)
        else:
            df[col].where(pd.notnull(df[col]), None, inplace=True)

    return df
