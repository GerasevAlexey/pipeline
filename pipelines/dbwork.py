import sqlite3
import csv
import pandas

## http://hello.com/home --> hello.com
def get_domain(url):
    return url.split("://")[1].split("/")[0]


def execSQL(query, connection='SMBD.db'):
    data = sqlite3.connect(connection)
    data.execute(query)
    data.commit()
    data.close()


def create(table, query, connection='SMBD.db'):
    data = sqlite3.connect(connection)
    data.create_function("domain", 1, get_domain)
    data.execute("create table if not exists " + table + " as " + query)
    data.close()
    

def save(file, table, connection='SMBD.db'):
    with open(f"{file}.csv", "w", newline='') as file:
        cur = sqlite3.connect(connection).cursor()
        writer = csv.writer(file)
        writer.writerow(['id', 'name', 'url', 'domain'])
        data = cur.execute("SELECT * FROM " + table)
        writer.writerows(data)


def load(file, table, connection='SMBD.db'):
    data = sqlite3.connect(connection)
    pandas.read_csv(f'{file}').to_sql(name=table, con=data, if_exists='append', index=False)
    data.close()