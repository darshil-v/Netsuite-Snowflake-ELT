import snowflake.connector as sfc
import pyodbc


def get_sf_connection(snowflake):
    """
    Establishes a connection to Snowflake database.

    Args:
        snowflake (dict): A dictionary containing the Snowflake connection parameters.

    Returns:
        sf_connection: The Snowflake database connection object, or -1 if connection fails.

    """
    try:
        sf_connection = sfc.connect(
            user=snowflake["user"],
            password=snowflake["password"],
            account=snowflake["account"],
            role=snowflake["role"],
            warehouse=snowflake["warehouse"],
        )
        return sf_connection
    except ConnectionError as e:
        print(e)
        return -1


def get_ns_connection(netsuite):
    """
    Establishes a connection to NetSuite database.

    Args:
        netsuite (dict): A dictionary containing the NetSuite connection parameters.

    Returns:
        ns_connection: The NetSuite database connection object, or -1 if connection fails.

    """
    driver = [item for item in pyodbc.drivers()][-1]

    try:
        ns_connection = pyodbc.connect(
            "DRIVER="
            + driver
            + ";Host="
            + netsuite["host"]
            + ";Port="
            + netsuite["port"]
            + ";Encrypted=1;AllowSinglePacketLogout=1;Truststore=system;SDSN="
            + netsuite["sdsn"]
            + "; UID="
            + netsuite["username"]
            + ";PWD="
            + netsuite["password"]
            + ";CustomProperties=AccountID="
            + netsuite["accountid"]
            + ";RoleID="
            + netsuite["roleid"]
        )
        return ns_connection
    except ConnectionError as e:
        print(e)
        return -1
