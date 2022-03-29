# This file wil be have any functions for Postgre
import psycopg2
import time
from config import hostname, username, password_user, db_name, log_filename
Max_Count_Char_In_Word = 50


def create_table(table_name, host=hostname, user=username, password=password_user, database=db_name, log_file=log_filename):
    try:
        # Connect to exist database
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        time_now = (time.ctime(time.time()).split())
        log = open(log_file, 'a')
        log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: PostgreSQL connection opened."\n')
        log.close()
        with connection.cursor() as cursor:
            cursor.execute(
                f"""CREATE TABLE {table_name}(
                id serial PRIMARY KEY,
                english_word varchar({str(Max_Count_Char_In_Word)}) NOT NULL,
                russian_word varchar({str(Max_Count_Char_In_Word)}) NOT NULL);"""
            )
            connection.commit()
            time_now = (time.ctime(time.time()).split())
            log = open(log_file, 'a')
            log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: Create a new table "{table_name}"\n')
            log.close()
    except Exception as _ex:
        time_now = (time.ctime(time.time())).split()
        log = open(log_file, 'a')
        log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[Error]: Create a new table "{table_name}". ' + str(_ex))
        log.close()
    finally:
        if connection:
            connection.close()
            time_now = (time.ctime(time.time())).split()
            log = open(log_file, 'a')
            log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: PostgreSQL connection closed.\n')
            log.close()


def add_word(english_word, russian_word, table_name, host=hostname, user=username, password=password_user,
             database=db_name, log_file=log_filename):
    try:
        # Insert data into a table
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        time_now = (time.ctime(time.time()).split())
        log = open(log_file, 'a')
        log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: PostgreSQL connection opened."\n')
        log.close()
        with connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO {table_name} (english_word, russian_word) VALUES ('{english_word}', '{russian_word}');"""
            )
            connection.commit()
            time_now = (time.ctime(time.time()).split())
            log = open(log_file, 'a')
            log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: Insert a new pair: "{english_word}" - '
                      f'"{russian_word}"\n')
            log.close()
    except Exception as _ex:
        time_now = (time.ctime(time.time())).split()
        log = open(log_file, 'a')
        log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[Error]: Insert a new pair: "{english_word}" - '
                  f'"{russian_word}". ' + str(_ex))
        log.close()
    finally:
        if connection:
            connection.close()
            time_now = (time.ctime(time.time())).split()
            log = open(log_file, 'a')
            log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: PostgreSQL connection closed.\n')
            log.close()


def delete_word(english_word, table_name, host=hostname, user=username, password=password_user, database=db_name,
                log_file=log_filename):
    try:
        # Delete a word
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        time_now = (time.ctime(time.time()).split())
        log = open(log_file, 'a')
        log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: PostgreSQL connection opened."\n')
        log.close()
        with connection.cursor() as cursor:
            cursor.execute(
                f"""DELETE FROM {table_name} where english_word = '{english_word}';"""
            )
        connection.commit()
        time_now = (time.ctime(time.time()).split())
        log = open(log_file, 'a')
        log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: Delete word "{english_word}"\n')
        log.close()
    except Exception as _ex:
        time_now = (time.ctime(time.time())).split()
        log = open(log_file, 'a')
        log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[Error]: Delete word "{english_word}". ' + str(_ex))
        log.close()
    finally:
        if connection:
            connection.close()
            time_now = (time.ctime(time.time())).split()
            log = open(log_file, 'a')
            log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: PostgreSQL connection closed.\n')
            log.close()


def get_info(table_name, host=hostname, user=username, password=password_user, database=db_name,
             log_file=log_filename):
    try:
        # Get all information from table
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        time_now = (time.ctime(time.time()).split())
        log = open(log_file, 'a')
        log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: PostgreSQL connection opened."\n')
        log.close()
        cursor = connection.cursor()
        cursor.execute(f"""SELECT * FROM {table_name};""")
        connection.commit()
        return_info = cursor.fetchall()
        time_now = (time.ctime(time.time()).split())
        log = open(log_file, 'a')
        log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: Get all information from {table_name}.\n')
        log.close()
    except Exception as _ex:
        time_now = (time.ctime(time.time())).split()
        log = open(log_file, 'a')
        log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[Error]: Get all information from {table_name}. ' + str(_ex))
        log.close()
    finally:
        if connection:
            connection.close()
            time_now = (time.ctime(time.time())).split()
            log = open(log_file, 'a')
            log.write(f'{time_now[1]} {time_now[2]} {time_now[3]}[CORRECT]: PostgreSQL connection closed.\n')
            log.close()
            return return_info
        else:
            return None
