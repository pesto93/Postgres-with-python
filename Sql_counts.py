import psycopg2
import logging
import os
from time import time
import csv


def create_csv(filename):
    if os.path.isfile(filename):
        print('Result file already exist')
    else:
        print('Creating result file')
        with open(filename, 'w+') as myfile:
            wr = csv.writer(myfile, delimiter=";", lineterminator="\n")
            wr.writerow(("Column", "Not Null", "Not_Null %"))


def save_csv(main_data, filename):
    with open(filename, 'a') as myfile:
        try:
            wr = csv.writer(myfile, delimiter=";", lineterminator="\n")
            wr.writerow(main_data)
        except:
            print('Error while adding new line')


def queries():
    """
    Choose your destiny
    :return: all chosen queries you want
    """
    query_col_name1 = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"
    query_col_name2 = "' ORDER BY ORDINAL_POSITION"
    query_count1 = "SELECT COUNT(*) FROM "
    query_count2 = " is not null"
    select_query = "SELECT * FROM "
    set_schema = "SET search_path TO rocket_data_raw"
    set_role = "SET role employees"
    return query_col_name1, query_col_name2, query_count1, query_count2


def close_con(con_cursor, con_connection):
    con_cursor.close()
    con_connection.close()
    print("PostgreSQL connection is closed")


def manage_connection():
    pg_user = 'urs'
    pg_password = 'urs'
    pg_host = 'urs'
    pg_port = 'urs'
    db_name = 'urs'
    schema = 'urs'
    con = psycopg2.connect(user=pg_user, password=pg_password, host=pg_host, port=pg_port, database=db_name)
    main_cursor = con.cursor()
    return con, main_cursor


def do_query(cursor, table, total, output):
    print("\nChecking the '" + table + "' table ...")
    q1, q2, q3, q4 = queries()
    column_name_query = q1 + table + q2
    cursor.execute(column_name_query)
    raw = cursor.fetchall()

    cols = [x[0] for x in raw]
    for c in cols:
        print("- Checking column '" + c + "' ...")
        not_null_query = q3 + table + ' WHERE ' + c + q4
        # not null values count
        cursor.execute(not_null_query)
        nulls = cursor.fetchone()[0]

        # log and append result in csv
        null_percent = "{: 10.2f}".format(nulls / total * 100)
        save_csv([c, nulls, null_percent + '%'], output, )
        print("--> Not null values " + c + ":   " + "{:,}".format(nulls) + '/' + "{:,}".format(total) + ' | ' + null_percent + ' %\n')


def execute():
    try:
        # tables in 'rocket_raw' schema with total row count
        # Note - you can decide to query every table total count if you decide but i didnt. i explicitly hard coded it :)
        tables = {'Employee1': 49937443, 'Employee2': 161776046, 'Employee3': 55300300, 'Employee4': 228049317, 'Employee5': 190534019,
                  'Employee6': 146851802}

        connection, cursor = manage_connection()
        if connection:
            print("\n ---------------------------------------- Connection opened for ------------------------------------------------------- \n")
            print(connection.get_dsn_parameters(), "\n")
            cursor.execute("SELECT version();")
            record = cursor.fetchall()
            print("You are connected to - ", record, "\n")

            # Operation starts now -----------------------------------------------------------------------------------------------------------
            # *****************************************************
            # SET UR SEARCH PATH TO YOUR SCHEMA HERE
            # *****************************************************
            cursor.execute("SET search_path TO ************UR SCHEMA********************")
            for table, total in tables.items():
                filename = os.getcwd() + os.sep + 'Output' + os.sep + 'File_' + table + '.csv'
                print(filename)
                create_csv(filename)
                do_query(cursor, table, total, filename)
        close_con(cursor, connection)

    except psycopg2.DatabaseError as db_error:
        print("Error while connecting to PostgreSQL ", db_error)
        pass


start = time()
execute()
end = time()
print("TIme {}'s".format(end - start))
