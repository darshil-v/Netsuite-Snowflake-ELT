def transient_landing_tables(sf_cnxn, PRIMARY_KEY_TABLES, props):
    LANDING_DB = props["LANDING_DB"]
    LANDING_SCHEMA = props["LANDING_SCHEMA"]
    TRANSIENT_SCHEMA = props["TRANSIENT_SCHEMA"]

    for table_name in PRIMARY_KEY_TABLES.keys():
        try:
            # CREATE TRANSIENT TABLE landing_transient.transient_table_name
            # CLONE landing.table_name;
            transient_ddl_query = f"""
                    CREATE OR REPLACE TRANSIENT TABLE {LANDING_DB}.{TRANSIENT_SCHEMA}.{table_name}
                    CLONE {LANDING_DB}.{LANDING_SCHEMA}.{table_name};
                    """
            with sf_cnxn.cursor() as sf_cur:
                sf_cur.execute(transient_ddl_query)
                print(sf_cur.fetchone()[0])

        except Exception as e:
            print(table_name, ":", e)
            continue
