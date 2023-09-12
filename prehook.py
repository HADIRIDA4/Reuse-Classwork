import os
from database_handler import (
    execute_query,
    create_connection,
    close_connection,
    return_data_as_df,
    return_create_statement_from_df,
    return_insert_into_sql_statement_from_df,
)
from lookups import (
    ErrorHandling,
    PreHookSteps,
    SQLTablesToReplicate,
    InputTypes,
    SourceName,
)
from logging_handler import show_error_message


def execute_sql_folder(db_session, sql_command_directory_path):
    sql_files = [
        sqlfile
        for sqlfile in os.listdir(sql_command_directory_path)
        if sqlfile.endswith(".sql")
    ]
    sorted_sql_files = sorted(sql_files)
    for sql_file in sorted_sql_files:
        with open(os.path.join(sql_command_directory_path, sql_file), "r") as file:
            sql_query = file.read()
            return_val = execute_query(db_session=db_session, query=sql_query)
            if not return_val == ErrorHandling.NO_ERROR:
                raise Exception(
                    f"{PreHookSteps.EXECUTE_SQL_QUERY.value} = SQL File Error on SQL FILE = "
                    + str(sql_file)
                )


def execute_csv_folder(csv_folder_path, db_session):
    csv_files = []
    csv_name = []
    for file in os.listdir(csv_folder_path):
        if file.endswith(".csv"):
            csv_files.append(os.path.join(csv_folder_path, file))  # Include full path
            csv_name.append(file)
    csv_files = sorted(csv_files)
    csv_name = sorted(csv_name)
    for file_idx in range(len(csv_files)):
        df = return_data_as_df(csv_files[file_idx], InputTypes.CSV, db_session=None)

        # Replace spaces in column names with underscores
        df.columns = [col.replace(" ", "_") for col in df.columns]

        statement = return_create_statement_from_df(
            df, SourceName.Store.value, csv_name[file_idx]
        )
        execute_query(db_session=db_session, query=statement)
        insert_statements = return_insert_into_sql_statement_from_df(
            df, SourceName.Store.value
        )
        for insert_statement in insert_statements:
            execute_query(db_session=db_session, query=insert_statement)


def file_executor(sql_command_directory_path, csv_file_path=None):
    db_session = create_connection()

    try:
        execute_sql_folder(db_session, sql_command_directory_path)

        if csv_file_path:
            execute_csv_folder(csv_file_path, db_session)

    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.FILE_EXECUTOR_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("File Executor Execution Failed")

    finally:
        close_connection(db_session)


def return_tables_by_schema(schema_name):
    schema_tables = list()
    tables = [table.value for table in SQLTablesToReplicate]
    for table in tables:
        if table.split(".")[0] == schema_name:
            schema_tables.append(table)
    return schema_tables


def create_sql_staging_tables(db_session, source_name):
    tables = return_tables_by_schema(source_name)
    for table in tables:
        staging_query = f"""
                SELECT * FROM {source_name}.{table} LIMIT 1
        """
        staging_df = return_data_as_df(
            db_session=db_session,
            input_type=InputTypes.SQL,
            file_executor=staging_query,
        )
        dst_table = f"stg_{source_name}_{table}"
        create_stmt = return_create_statement_from_df(
            staging_df, "dw_reporting", dst_table
        )
        execute_query(db_session=db_session, query=create_stmt)


def execute_prehook(sql_command_directory_path="./SQL_Commands"):
    try:
        db_session = create_connection()
        # Step 1:
        file_executor(db_session, sql_command_directory_path)
        # Step 2 getting dvd rental staging:
        create_sql_staging_tables(db_session, SourceName.DVD_RENTAL)
        # Step 3 getting college staging:
        # create_sql_staging_tables(db_session,SourceName.COLLEGE)
        close_connection(db_session)
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.PREHOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")
