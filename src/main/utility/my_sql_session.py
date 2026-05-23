import mysql.connector

def get_mysql_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Golu@4321",
        database="manish"
    )
    return connection



