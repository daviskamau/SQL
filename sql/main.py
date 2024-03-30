# import libraries

import mysql.connector
from mysql.connector import Error
import pandas as pd
from IPython.display import display

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try: 
        cursor.execute(query)
        print("Database successfully created")
    except Error as err:
        print(f"Error: '{err}'")

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            passwd = user_password,
            database = db_name)
        print("MySQL database connection successful")
    except Error as err:
        print(f"Error: '{err}")
    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query was successful")
    except Error as err:
        print(f"Error: '{err}'")

def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")

def execute_and_print_query(query):
    results = read_query(connection, query)
    for result in results:
        print(result)
    return results

pw = "Easy-to-remember"
db = "mysql_python"
create_database_query = "Create database mysql_python"
create_orders_table = """
create table orders(
order_id int primary key,
customer_name varchar(30) not null,
product_name varchar(20) not null,
data_ordered date,
quantity int,
unit_price float,
phone_number varchar(20));
"""
data_orders = """
insert into orders values
(101, 'Steve', 'Laptop', '2024-02-12', 2, 800, '6293730802'),
(102, 'Jos', 'Books', '2024-02-14', 10, 12, '8367489124'),
(103, 'Stacy', 'Trousers', '2024-01-25', 5, 50, '8976123645'),
(104, 'Nancy', 'T-Shirts', '2023-12-14', 7, 30, '7368145099'),
(105, 'Maria', 'Headphones', '2024-01-02', 6, 48, '8865316698'),
(106, 'Danny', 'Smart TV', '2023-08-20', 10, 300, '7720130449');
"""
q = []
q.append("""
select * from orders;
""")
q.append("""
select customer_name, phone_number from orders;
""")
q.append("""
select year(date_ordered) from orders;
""")
q.append("""
select distinct year(date_ordered) from orders;
""")
q.append("""
select * from orders where date_ordered < '2024-03-30';
""")
q.append("""
select * from orders where date_ordered > '2024-03-30';
""")
q.append("""
select * from orders order by unit_price;
""")
q.append("""
select * from orders where order_id = 103;
""")
q.append("""
select * from orders;
""")

update = """
update orders 
set unit_price = 45 
where order_id = 103
"""
delete_order = """
delete from orders
where order_id = 105
"""




# Connect to server and database
connection = create_server_connection("localhost", "root", pw)
connection = create_db_connection("localhost", "root", pw, db)

# Create and edit database
create_database(connection, create_database_query)
connection = create_db_connection("localhost", "root", pw, db)
execute_query(connection, create_orders_table)
execute_query(connection, data_orders)

# Extract information from database
for i in range(0, 6):
    print("\n query{}: ".format(i+1))
    results = execute_and_print_query(q[i])

# Import table as dataframe
from_db = []
for result in results:
    result = list(result)
    from_db.append(result)
columns = ["order_id", "customer_name", "procuct_name", "date_ordered", "quantity",
            "unit price", "phone_number"]
df = pd.DataFrame(from_db, columns = columns)
display(df)

# Update and delete entries
execute_query(connection, update)
results = execute_and_print_query(q[7])
execute_query(connection, delete_order)
results = execute_and_print_query(q[8])