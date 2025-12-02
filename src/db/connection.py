import sqlite3

def get_connection(db_path="data/database.db"):
    return sqlite3.connect(db_path)