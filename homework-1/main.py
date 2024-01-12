"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2

FILE_CUSTOMERS = 'north_data/customers_data.csv'
FILE_EMPLOYEES = 'north_data/employees_data.csv'
FILE_ORDERS = 'north_data/orders_data.csv'


def import_date(filename, tabl):
    try:
        with open(filename, 'r', encoding='UTF-8') as file:
            data = csv.DictReader(file)
            with psycopg2.connect(host="localhost",
                                  database="north",
                                  user="postgres",
                                  password="12345") as conn:
                with conn.cursor() as cur:
                    for row in data:
                        row_ = []
                        values_tabl = []
                        for key in list(row.keys()):
                            row_.append(row[key])
                            values_tabl.append('%s')
                        cur.execute(f"INSERT INTO {tabl} VALUES ({', '.join(values_tabl)})",
                                    tuple(row_))
                        row_.clear()
                        values_tabl.clear()
    finally:
        conn.close()


import_date(FILE_CUSTOMERS, 'customers')
import_date(FILE_EMPLOYEES, 'employees')
import_date(FILE_ORDERS, 'orders')
