import os
import sys
import sqlite3
from sqlite3 import Error

"""Returns the name of the SQLite DB file"""


def getbasefile():
    return os.path.splitext(os.path.basename(__file__))[0]


"""Connects to the SQLite DB"""


def connectdb():
    try:
        dbfile = getbasefile() + '.db'
        conn = sqlite3.connect(dbfile, timeout=2)
        print("Connection is established: Database is created in memory")
    except Error:
        print(Error)
    finally:
        conn.close()
