import psycopg2
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from getpass import getpass
from sqlalchemy import create_engine, table, column
from sqlalchemy.dialects.postgresql import insert


""""
Here pandas is used for reading the csv data and dealing with the data itself.
psycopg2 is the database adapter used for working with PostgreSQL.
sqlalchemy is used for easy insertion of the data into the database.
getpass is used for prompting user password

Note that there are several duplicate entries in the provided data such as:
DETAIL:  Key (id, arrival_date)=(5602329, 2006-07-12) already exists.
I will assume that duplicate entries with regards to the compound primary key
should be omitted. 
"""

# Create connection, specify your own username and password
username= input("Enter username: ")
password = getpass("Enter password (hidden): ")
connection = psycopg2.connect(host="localhost", user=username, password=password)
connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

# Get DB cursor
cursor = connection.cursor()

# Drop and create the DB
db_name = "patients"
cursor.execute('DROP DATABASE IF EXISTS ' + db_name)
cursor.execute('CREATE DATABASE patients')
connection.commit()
connection.close()

# Create a new connection, this time to the newly created DB
connection = psycopg2.connect(host="localhost", database=db_name, user=username, password=password)
cursor = connection.cursor()

# Create the DB table
table_name = "patient_visits"

sqlCreateTable = "CREATE TABLE " + table_name + " (id integer, arrival_date date, release_date date, ward varchar, weight real, PRIMARY KEY (id, arrival_date))"
cursor.execute(sqlCreateTable)
connection.commit()

# Read, treat and filter data
data = pd.read_csv("entities.csv", index_col=False)
data.columns = ["id", "arrival_date", "release_date", "ward", "weight"]

# Drop duplicates based on compound primary key 
# data = data.drop_duplicates(subset=["id", "arrival_date"], keep="first")

# Convert string dates to datetime64, easier to work with for filtering
data['arrival_date'] = pd.to_datetime(data['arrival_date'], format='%d.%m.%Y')
data['release_date'] = pd.to_datetime(data['release_date'], format='%d.%m.%Y')

filtered_data = data.loc[data["arrival_date"] >= "7.6.2006"]

# Convert back to dates. Raises some pandas warnings but it is fine.
filtered_data['arrival_date'] = filtered_data['arrival_date'].dt.date
filtered_data['release_date'] = filtered_data['release_date'].dt.date

# SQLAlchemy 'engine' used to conveniently insert pandas dataframes into DB tables
engine = create_engine('postgresql+psycopg2://' + username + ":" + password + "@localhost/" + db_name)

# Custom method for performing the SQL insertion, in this case we 
# want to do nothing on primary key conflicts
def insert_do_nothing_on_conflicts(sqltable, conn, keys, data_iter):
    
    # Construct table from data to be inserted
    columns=[]
    for c in keys:
        columns.append(column(c))
    if sqltable.schema:
        table_name = '{}.{}'.format(sqltable.schema, sqltable.name)
    else:
        table_name = sqltable.name
    mytable = table(table_name, *columns)

    insert_statement = insert(mytable).values(list(data_iter))
    confl_insert_statement = insert_statement.on_conflict_do_nothing(index_elements=['id', 'arrival_date'])

    conn.execute(confl_insert_statement)

# Insert the data into the DB table
filtered_data.to_sql(table_name, engine, if_exists='append', index=False, method=insert_do_nothing_on_conflicts)

connection.commit()
connection.close()