import sqlite3
from sqlite3 import Error

def create_connection(db_file):
   """ create a database connection to the SQLite database
       specified by db_file
   :param db_file: database file
   :return: Connection object or None
   """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       return conn
   except Error as e:
       print(e)

   return conn

def execute_sql(conn, sql):
   """ Execute sql
   :param conn: Connection object
   :param sql: a SQL script
   :return:
   """
   try:
       c = conn.cursor()
       c.execute(sql)
   except Error as e:
       print(e)

def add_employee(conn, employee):
   """
   Create a new project into the projects table
   :param conn:
   :param employee:
   :return: employee id
   """
   sql = '''INSERT INTO employees(employee_id, first_name, last_name, position, performance)
             VALUES(?,?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, employee)
   conn.commit()
   return cur.lastrowid

def add_furniture(conn, furniture):
   """
   Create a new task into the furniture table
   :param conn:
   :param furniture:
   :return: furniture id
   """
   sql = '''INSERT INTO furniture(employee_id, model_name, date_of_production, price)
             VALUES(?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, furniture)
   conn.commit()
   return cur.lastrowid

def select_all(conn, table):
   """
   Query all rows in the table
   :param conn: the Connection object
   :return:
   """
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()

   return rows

def select_where(conn, table, **query):
   """
   Query tasks from table with data from **query dict
   :param conn: the Connection object
   :param table: table name
   :param query: dict of attributes and values
   :return:
   """
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows

def update(conn, table, id, **kwargs):
   """
   update status, begin_date, and end date of a task
   :param conn:
   :param table: table name
   :param id: row id
   :return:
   """
   parameters = [f"{k} = ?" for k in kwargs]
   parameters = ", ".join(parameters)
   values = tuple(v for v in kwargs.values())
   values += (id, )

   sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
   try:
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("OK")
   except sqlite3.OperationalError as e:
       print(e)

def delete_where(conn, table, **kwargs):
   """
   Delete from table where attributes from
   :param conn:  Connection to the SQLite database
   :param table: table name
   :param kwargs: dict of attributes and values
   :return:
   """
   qs = []
   values = tuple()
   for k, v in kwargs.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)

   sql = f'DELETE FROM {table} WHERE {q}'
   cur = conn.cursor()
   cur.execute(sql, values)
   conn.commit()
   print("Deleted")

def delete_all(conn, table):
   """
   Delete all rows from table
   :param conn: Connection to the SQLite database
   :param table: table name
   :return:
   """
   sql = f'DELETE FROM {table}'
   cur = conn.cursor()
   cur.execute(sql)
   conn.commit()
   print("Deleted")

if __name__ == '__main__':

    create_employee_sql = """
    CREATE TABLE IF NOT EXISTS employees
    (
        id integer PRIMARY KEY,
        employee_id integer NOT NULL,
        first_name text NOT NULL,
        last_name text NOT NULL,
        position text NOT NULL,
        performance float
    );
    """

    create_furniture_sql = """
    CREATE TABLE IF NOT EXISTS furniture
    (
        id integer PRIMARY KEY,
        employee_id integer NOT NULL,
        model_name text NOT NULL,
        date_of_production integer,
        price float,
        FOREIGN KEY (employee_id) REFERENCES employee (id)
    );
    """

    db_file = "database.db"
    conn = create_connection(db_file)

    if conn is not None:
        execute_sql(conn, create_employee_sql)
        execute_sql(conn, create_furniture_sql)

    employee = (
        1020557,
        "Karol",
        "Byczek",
        "Monter",
        80
        )
    employee_2 = (
        1020560,
        "Dawid",
        "Czekan",
        "Stolarz",
        50
        )
    employee_3 = (
        1020571,
        "Marta",
        "Dawidowicz",
        "Szwaczka",
        99
        )
    employee_4 = (
        1020600,
        "Robert",
        "Markiewicz",
        "Tapicer",
        20
        )


    furniture = (
        1020583,
        "Savanah",
        "2022.06.15",
        5010
        )
    furniture_2 = (
        1020571,
        "Teracota",
        "2022.06.14",
        8000
        )
    furniture_3 = (
        1020560,
        "Ivon",
        "2022.06.07",
        7000
        )


    var_1 = add_employee(conn, employee)
    var_2 = add_furniture(conn, furniture)
    var_3 = add_employee(conn, employee_2)
    var_4 = add_furniture(conn, furniture_2)
    var_5 = add_employee(conn, employee_3)
    var_6 = add_furniture(conn, furniture_3)
    var_7 = add_employee(conn, employee_4)

    print(var_1, var_2, var_3, var_4, var_5, var_6, var_7)
    print(select_where(conn, "employees", first_name="Dorota"))

    update(conn, "employees", 3, first_name="Dorota")

    print(select_where(conn, "employees", first_name="Dorota"))
    print(select_where(conn, "employees", first_name="Robert"))
    delete_where(conn, "employees", first_name="Robert")
    print(select_where(conn, "employees", first_name="Robert"))

