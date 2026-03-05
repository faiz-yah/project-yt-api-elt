from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import parse_duration, transform_data
from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = 'yt_api'

@task
def staging_table():
    
    schema = 'staging'
    
    conn,cur = None, None
    
    try:
        conn,cur = get_conn_cursor()
        
        # Fetch data with API
        YT_data = load_data()
        
        # Initialise schema and table (if not exist yet)
        create_schema(schema)
        create_table(schema)
        
        # Obtain ids from stored data above through postgres query
        table_ids = get_video_ids(cur, schema)
        
        # Insert data into database
        for row in YT_data:
            
            # First time insert when table is empty
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)
            
            # Upsert operation    
            else:
                if row['video_id'] in table_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)
            
            ## Staging table sync: Mirror exactly what is in the incoming JSON, no more no less
            
            # New incoming data
            ids_in_json = {row['video_id'] for row in YT_data}
            
            # Full Refresh Sync : (Existing data in the staging database) - (New incoming data) = Removed stale records
            ids_to_delete = set(table_ids) - ids_in_json
            
            # If ids_to_delete not null
            if ids_to_delete:
                delete_rows(cur, conn, schema, ids_to_delete)
            
            logger.info(f"{schema} table update completed")
     
    except Exception as e:
        logger.error(f"An error occured during the update of {schema} - table: {e}")      
        raise e
    
    finally:
        # If conn and cur not null
        if conn and cur:
            close_conn_cursor(conn, cur)



@task
def core_table():
    
    schema = 'core'
    
    conn,cur = None, None
    
    try:
        conn,cur = get_conn_cursor()
        
        # Fetch data with API
        YT_data = load_data()
        
        # Initialise schema and table (if not exist yet)
        create_schema(schema)
        create_table(schema)
        
        # Obtain ids from stored data above through postgres query
        table_ids = get_video_ids(cur, schema)
        
        current_video_ids = set()
        
        # Fetch all data from staging table
        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()
        
        for row in rows:
            
            # Add 
            current_video_ids.add(row["Video_ID"])
            
            if len(table_ids) == 0:
                tranformed_row = transform_data(row)
                
                # Upsert
                if tranformed_row["Video_ID"] in table_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)
            
            ids_to_delete = set(table_ids) -  current_video_ids
            
            # If ids_to_delete not null
            if ids_to_delete:
                delete_rows(cur, conn, schema, ids_to_delete)
            
            logger.info(f"{schema} table update completed")
     
    except Exception as e:
        logger.error(f"An error occured during the update of {schema} - table: {e}")      
        raise e
    
    finally:
        # If conn and cur not null
        if conn and cur:
            close_conn_cursor(conn, cur)