from configparser import ConfigParser
from conn_util import get_sf_connection, get_ns_connection
from tables import *
from load_tables import load_tables
from bulk_load import bulk_load
from incremental_load import incremental_load
from landing_to_staging import landing_to_staging
from staging_to_datamart import staging_to_datamart
from incremental_load_transient import incremental_load_transient
import argparse

parser = argparse.ArgumentParser(
    description="Parser for Infofiscus-Python",
    formatter_class=argparse.RawTextHelpFormatter,
)

parser.add_argument(
    "phase",
    type=int,
    choices=range(0, 4),
    
    help="""Phase # of the operation:
	0: NetSuite to Landing - Create tables + Bulk Load
	1: NetSuite to Landing - Incremental Load
	2: Landing to Staging
	3: Staging to Datamart
""",
)
args = parser.parse_args()
phase_config = vars(args)
PHASE_ID = phase_config["phase"]

if __name__ == "__main__":
    config = ConfigParser()
    config.read("properties/conn_props.ini")
    ns_config = config["netsuite"]
    sf_config = config["snowflake"]

    ns_cnxn = get_ns_connection(ns_config)
    sf_cnxn = get_sf_connection(sf_config)

    # PHASE_ID = 0 -> NetSutie to Snowflake Landing (Bulk)
    # PHASE_ID = 1 -> NetSuite to Snowflake Landing (Incremental)
    # PHASE_ID = 2 -> Snowflake Landing to Staging
    # PHASE_ID = 3 -> Snowflake Staging to DataMart
    # PHASE_ID = 1

    NETSUITE_TABLES = getNetsuiteTables()
    PRIMARY_KEY_TABLES = getPrimaryKeyTables()
    SF_DATATYPES = getDataTypes()
    SOURCE_VIEW_KEYS = getSourceViewKeys()

    if ns_cnxn != -1 and sf_cnxn != -1:
        try:
            if PHASE_ID == 0:
                props = dict(
                    {
                        "ENV": "INFOFISCUS_PYTHON_LANDING",
                        "CONTROL_TABLE": "INFOFISCUS_PYTHON_LANDING.PUBLIC.NETSUITE_CT",
                        "LANDING_DB": "INFOFISCUS_PYTHON_LANDING",
                        "LANDING_SCHEMA": "FINANCE",
                    }
                )
                # load_tables(ns_cnxn, sf_cnxn, NETSUITE_TABLES, PRIMARY_KEY_TABLES, SF_DATATYPES, props)
                # bulk_load(ns_cnxn, sf_cnxn, NETSUITE_TABLES, PRIMARY_KEY_TABLES, props)

            elif PHASE_ID == 1:
                props = dict(
                    {
                        "ENV": "INFOFISCUS_PYTHON_LANDING",
                        "CONTROL_TABLE": "INFOFISCUS_PYTHON_LANDING.PUBLIC.NETSUITE_CT",
                        "LANDING_DB": "INFOFISCUS_PYTHON_LANDING",
                        "LANDING_SCHEMA": "FINANCE",
                    }
                )
                # incremental_load(ns_cnxn, sf_cnxn, NETSUITE_TABLES, PRIMARY_KEY_TABLES, props)
                incremental_load_transient(
                    ns_cnxn, sf_cnxn, NETSUITE_TABLES, PRIMARY_KEY_TABLES, props
                )

            elif PHASE_ID == 2:
                props = dict(
                    {
                        "CONTROL_TABLE": "INFOFISCUS_PYTHON_STAGING.PUBLIC.STAGING_CT",
                        "LANDING_DB": "INFOFISCUS_PYTHON_LANDING",
                        "LANDING_SCHEMA": "FINANCE",
                        "STAGING_DB": "INFOFISCUS_PYTHON_STAGING",
                        "STAGING_SCHEMA": "FINANCE_STG",
                    }
                )
                landing_to_staging(sf_cnxn, props)

            elif PHASE_ID == 3:
                props = dict(
                    {
                        "STAGING_DB": "INFOFISCUS_PYTHON_STAGING",
                        "STAGING_SCHEMA": "FINANCE_STG",
                        "DATAMART_DB": "INFOFISCUS_PYTHON_DATAMART",
                        "DATAMART_SCHEMA": "FINANCE",
                    }
                )
                res = staging_to_datamart(sf_cnxn, props, SOURCE_VIEW_KEYS)
                if res == -1:
                    print("Snowflake Staging to DataMart Failed!!!")
                else:
                    print("Snowflake Staging to DataMart Completed!!!")
            else:
                print("Invalid PHASE_ID:", PHASE_ID)

        except Exception as e:
            print(e)
        finally:
            ns_cnxn.close()
            sf_cnxn.close()
    else:
        print("Connection Error")
