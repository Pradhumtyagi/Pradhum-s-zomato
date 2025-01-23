from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime
import pandas as pd

POSTGRES_CONN_ID = 'postgres_default'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
}

# Define the DAG
with DAG(
    dag_id='Zomato_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    @task()
    def extract_data():
        file_path = "Customers.csv"
        file_path_1 = "Orders.csv"
        file_path_2 = "Restaurants.csv"

        # Load CSV files into DataFrames
        df = pd.read_csv(file_path)
        df1 = pd.read_csv(file_path_1)
        df2 = pd.read_csv(file_path_2)

        # Convert DataFrames to JSON-serializable formats
        return {
            "customers": df.to_dict(orient='records'),
            "orders": df1.to_dict(orient='records'),
            "restaurants": df2.to_dict(orient='records'),
        }
    @task()
    def create_database():
        # Initialize the PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Get a connection
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create the database
        cursor.execute("CREATE DATABASE IF NOT EXISTS zomato")

        # Commit changes
        conn.commit()

        # Close the connection
        conn.close()
    @task()
    def load_data_to_postgres(data):
        # Initialize the PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Define table names
        table_customer = 'customer_record'
        table_restaurant = 'restaurant_data'
        table_order = 'order_data'

        # Get a connection
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create Customer Table
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_customer} (
            Customer_ID VARCHAR(20) PRIMARY KEY,
            Customer_Location VARCHAR(50),
            Customer_Age_Group TEXT,
            Customer_Rating FLOAT,
            Customer_Name VARCHAR(50)
        );
        """)

        # Insert Customer Data
        for customer in data['customers']:
            cursor.execute(f"""
            INSERT INTO {table_customer} (Customer_ID, Customer_Location, Customer_Age_Group, Customer_Rating, Customer_Name)
            VALUES (%s, %s, %s, %s, %s)
            """, (
                customer['Customer_ID'],
                customer['Customer_Location'],
                customer['Customer_Age_Group'],
                customer['Customer_Rating'],
                customer['Customer_Name']
            ))

        # Commit changes
        conn.commit()

        # Create Restaurant Table
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_restaurant} (
            Restaurant_ID VARCHAR(20),
            Name VARCHAR(50),  
            Location VARCHAR(50),  
            Cuisine_Types VARCHAR(50),
            Avg_Cost_for_Two INT,
            Ratings FLOAT,
            Reviews_Count INT,
            Operational_Hours VARCHAR(50)
        );
        """)

        # Insert Restaurant Data
        for restaurant in data['restaurants']:
            cursor.execute(f"""
            INSERT INTO {table_restaurant} (Restaurant_ID, Name, Location, Cuisine_Types, Avg_Cost_for_Two, Ratings, Reviews_Count, Operational_Hours)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
            """, (
                restaurant['Restaurant_ID'],
                restaurant['Name'],
                restaurant['Location'],
                restaurant['Cuisine_Types'],
                restaurant['Avg_Cost_for_Two'],
                restaurant['Ratings'],
                restaurant['Reviews_Count'],
                restaurant['Operational_Hours']
            ))

        # Commit changes
        conn.commit()

        # Create Order Table with Foreign Keys
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_order} (
            Order_ID VARCHAR(20) PRIMARY KEY,
            Restaurant_ID VARCHAR(20),
            Order_Date VARCHAR(50),
            Expected_Delivery_Time INT,
            Actual_Delivery_Time INT,
            Total_Amount FLOAT,
            Order_Status VARCHAR(50),
            Payment_Method VARCHAR(50),
            Dish_Name VARCHAR(50),
            Customer_ID VARCHAR(20),
            FOREIGN KEY (Customer_ID) REFERENCES {table_customer}(Customer_ID),
            FOREIGN KEY (Restaurant_ID) REFERENCES {table_restaurant}(Restaurant_ID)
        );
        """)

        # Insert Order Data
        for order in data['orders']:
            cursor.execute(f"""
            INSERT INTO {table_order} (Order_ID, Restaurant_ID, Order_Date, Expected_Delivery_Time, Actual_Delivery_Time, Total_Amount, Order_Status, Payment_Method, Dish_Name, Customer_ID)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order['Order_ID'],
                order['Restaurant_ID'],
                order['Order_Date'],
                order['Expected_Delivery_Time'],
                order['Actual_Delivery_Time'],
                order['Total_Amount'],
                order['Order_Status'],
                order['Payment_Method'],
                order['Dish_Name'],
                order['Customer_ID']
            ))

        # Commit changes
        conn.commit()

        # Close cursor
        cursor.close()

    # Task dependencies
    data = extract_data()
    create_database()
    load_data_to_postgres(data)
