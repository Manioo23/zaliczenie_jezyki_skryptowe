import sqlite3
from sqlite3 import Error

class DataBaseProvider:
    def __init__(self, db_file="re_book.db"):
        self.conn = None
        self.db_name = db_file
        self.__init_database()

    def __create_table(self, query):
        try:
            self.__open_connection()
            cur = self.conn.cursor()
            cur.execute(query)
            print('[DB] Table created successfully')
            self.__close_conection()
        except Error as e:
            print(e)

    def __close_conection(self):
        self.conn.commit()
        self.conn.close()

    def __open_connection(self):
        try:
            self.conn = sqlite3.connect(f'./{self.db_name}')
        except Error as e:
            print(e)

    def __init_database(self):
        sql_create_books_table = """ CREATE TABLE IF NOT EXISTS books (
                                        _IdB integer PRIMARY KEY,
                                        Name text NOT NULL,
                                        Author text NOT NULL,
                                        Year text NOT NULL,
                                        UNIQUE(Name, Author, Year)
                                    );"""                  
        sql_create_logs_table = """CREATE TABLE IF NOT EXISTS logs (
                                        _IdL integer PRIMARY KEY,
                                        DateTime text NOT NULL,
                                        Type text NOT NULL,
                                        IdU integer NOT NULL,
                                        FOREIGN KEY (IdU) REFERENCES users (_IdU)
                                    );"""
        sql_create_users_table = """CREATE TABLE IF NOT EXISTS users (
                                        _IdU integer PRIMARY KEY,
                                        Login text NOT NULL,
                                        Password text NOT NULL,
                                        Name text NOT NULL,
                                        UNIQUE(Login)
                                    );"""
        sql_create_opinions_table = """CREATE TABLE IF NOT EXISTS opinions (
                                        _IdO integer PRIMARY KEY,
                                        IdB integer NOT NULL,
                                        IdU integer NOT NULL,
                                        Payload text NOT NULL,
                                        Ranking integer NOT NULL,
                                        UNIQUE(IdB, IdU)
                                        FOREIGN KEY (IdU) REFERENCES users (_IdU)
                                        FOREIGN KEY (IdB) REFERENCES books (_IdB)

                                    );"""

        self.__create_table(sql_create_users_table)
        self.__create_table(sql_create_books_table)
        self.__create_table(sql_create_logs_table)
        self.__create_table(sql_create_opinions_table)

    def add_book(self, name, author, year):
        sql = f' INSERT INTO books(Name,Author,Year) VALUES("{name}","{author}","{year}");'
        try:
            self.__open_connection()
            cur = self.conn.cursor()
            cur.execute(sql)
        except Error as e:
            print(e)
        finally:
            self.__close_conection()
        
        return cur.lastrowid