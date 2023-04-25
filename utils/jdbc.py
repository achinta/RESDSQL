import jaydebeapi
from pydantic import BaseModel
from fastapi import HTTPException
from .sqlite import get_spider_db_dir
import sqlite3
from pathlib import Path

class JDBCData(BaseModel):
    db_id: str
    driver: str
    url: str
    username: str
    password: str
    jar: str

def preprocess_jdbc_conn(jdbc_data:JDBCData):
    # Connect to JDBC data source
    jdbc_data.db_id = '22'
    jdbc_data.driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
    jdbc_data.url = 'jdbc:sqlserver://finland.database.windows.net:1433;databaseName=finlanddb'
    jdbc_data.username = 'sqluser'
    jdbc_data.password = '2Cwmz8Jmz*b'
    jdbc_data.jar = '/jars/sqljdbc_12.2/enu/mssql-jdbc-12.2.0.jre11.jar'
    return jdbc_data

def get_columns_in_resultset(rs):
    metadata = rs.getMetaData()
    column_count = metadata.getColumnCount()
    column_names = []
    for i in range(1, column_count + 1):
        column_names.append(metadata.getColumnName(i))
    return column_names

def get_columns(db_meta, table_name):
    rs = db_meta.getColumns(None, None, table_name, None)
    table_cols = []
    while rs.next():
        column_name = rs.getString("COLUMN_NAME")
        data_type = rs.getString("TYPE_NAME")
        col_nullable = 'NOT NULL' if rs.getString('NULLABLE') == 'NO' else ''
        table_cols.append((column_name, data_type, col_nullable))
    return table_cols

def get_primary_keys(db_meta, table_name):
    rs = db_meta.getPrimaryKeys(None, None, table_name)
    primary_keys = []
    while rs.next():
        primary_keys.append(rs.getString('COLUMN_NAME'))
    return primary_keys


def jdbc_to_sqlite(jdbc_data:JDBCData):
    # Connect to JDBC data source
    sqlite_db_path = f'{get_spider_db_dir()}/{jdbc_data.db_id}/{jdbc_data.db_id}.sqlite'
    print(f'sqlite db path is {sqlite_db_path}')
    # create db dir
    Path(sqlite_db_path).parent.mkdir(parents=True, exist_ok=True)
    jdbc_conn = None
    try:
        jdbc_conn = jaydebeapi.connect(jdbc_data.driver, jdbc_data.url, [jdbc_data.username, jdbc_data.password], jdbc_data.jar)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"JDBC connection failed: {e}")

    # read jdbc metadata
    db_metadata = jdbc_conn.jconn.getMetaData()
    table_names = []
    table_result_set = db_metadata.getTables(None, None, None, ['TABLE'])
    while table_result_set.next():
        table_name = table_result_set.getString('TABLE_NAME')
        table_names.append(table_name)
 
    # Loop through tables and recreate schema in SQLite database
    sqlite_conn = sqlite3.connect(sqlite_db_path)
    cursor = sqlite_conn.cursor()
 
    for table_name in table_names:
        # Get table schema
        rs = db_metadata.getColumns(None, None, table_name, None)
        table_cols = get_columns(db_metadata, table_name)
        
        
        # Generate SQL script to recreate table schema
        col_names = ',\n'.join([f'{col_name} {col_type} {col_nullable}' for col_name, col_type, col_nullable in table_cols])
        sql_script = f'CREATE TABLE {table_name} (\n {col_names} \n)'
 
        # Add primary key constraint
        pk_cols = get_primary_keys(db_metadata, table_name)
        # print(f'pk_cols: {pk_cols}')
        pk_cols_str = ', '.join(pk_cols)
        # pk_cols = [pk[3] for pk in primary_keys]
        if len(pk_cols) > 0:
            pk_cols_str = ', '.join(pk_cols)
            sql_script += f'    PRIMARY KEY ({pk_cols_str})'
 
        # # Add foreign key constraints
        # fk = db_metadata.getImportedKeys(None, None, table_name)
        # while fk.next():
        #     fk_name = fk[7]
        #     fk_col = fk[8]
        #     ref_table = fk[2]
        #     ref_col = fk[3]
        #     sql_script += f',\n    FOREIGN KEY ({fk_col}) REFERENCES {ref_table}({ref_col})'
 
        # sql_script += '\n);'
 
        # Execute SQL script to recreate table schema
        # print(sql_script)
        cursor.execute(f'drop table if exists {table_name}')
        cursor.execute(sql_script)
 
    sqlite_conn.commit()
 
    # Clean up connections
    jdbc_conn.close()
    sqlite_conn.close()
    return jdbc_data.db_id

    

