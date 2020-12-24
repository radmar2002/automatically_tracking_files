import os
import sys
import sqlite3
from sqlite3 import Error


def getbasefile():
    """
    Returns the name of the SQLite DB file
    """
    return os.path.splitext(os.path.basename(__file__))[0]


def connectdb():
    """
    Connects to the SQLite DB
    """
    try:
        dbfile = getbasefile() + '.db'
        conn = sqlite3.connect(dbfile, timeout=2)
        print("Connection is established: Database is created on disk")
        return conn
    except Error:
        print(Error)


"""Check functionality"""
if __name__ == "__main__":
    conn = connectdb()
    cursor = conn.cursor()

    cursor.execute(
        "create table people (id integer primary key, name text, count integer)")
    cursor.execute("insert into people (name, count) values ('Marius', 1)")
    cursor.execute(
        "insert into people (name, count) values (?, ?)", ("Radu", 15))
    conn.commit()

    result = cursor.execute("SELECT * FROM people")
    for row in result:
        print(row)

    conn.commit()
    conn.close()
