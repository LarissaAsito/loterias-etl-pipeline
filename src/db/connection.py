import sqlite3

class ConnectionDB:
    def get_connection(self, db_path:str = "data/database.db") -> sqlite3.Connection:
        return sqlite3.connect(db_path)