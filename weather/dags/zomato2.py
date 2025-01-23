from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import pandas as pd

POSTGRES_CONN_ID = 'postgres_default'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 20),
}

# Logger for logging events
logger = LoggingMixin().log

# Define the DAG
with DAG(
    dag_id='Zomato_pipeline2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    @task()
    def extract_data():
        # Use Airflow Variables for file paths
        file_path = Variable.get("customer_file_path", default_var="Customers.csv")
        file_path_1 = Variable.get("orders_file_path", default_var="Orders.csv")
        file_path_2 = Variable.get("restaurants_file_path", default_var="Restaurants.csv")

        # Load CSV files into DataFrames
        try:
            df = pd.read_csv(file_path)
            df1 = pd.read_csv(file_path_1)
            df2 = pd.read_csv(file_path_2)
        except Exception as e:
            logger.error(f"Error reading CSV files: {e}")
            raise

        # Convert DataFrames to JSON-serializable formats
        return {
            "customers": df.to_dict(orient='records'),
            "orders": df1.to_dict(orient='records'),
            "restaurants": df2.to_dict(orient='records'),
        }

    @task()
    def create_database():
        from psycopg2 import connect, sql
        from airflow.hooks.base import BaseHook

        try:
            # Get the connection details from Airflow
       
            connection = BaseHook.get_connection(POSTGRES_CONN_ID)

            # Connect to the default 'postgres' database
       
            conn = connect(
                dbname="postgres",
                user=connection.login,
                password=connection.password,
                host=connection.host,
                port=connection.port
            )
         

            conn.autocommit = True  # Disable transaction block
            cursor = conn.cursor()

            # Check if the database exists
    
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'zomato'")
            exists = cursor.fetchone()
            if not exists:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier("zomato")))
                logger.info("Database 'zomato' created successfully.")
              
                logger.info("Database 'zomato' already exists.")
               
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            raise

         
    
        finally:
            cursor.close()
            conn.close()
    

    # @task()
    # def create_database():
    #     # Initialize the PostgresHook
    #     pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    #     # Get a connection and check if the database exists
    #     try:
    #         conn = pg_hook.get_conn()
    #         cursor = conn.cursor()
    #         cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'zomato'")
    #         exists = cursor.fetchone()
    #         if not exists:
    #             cursor.execute("CREATE DATABASE zomato")
    #             logger.info("Database 'zomato' created successfully.")
    #         else:
    #             logger.info("Database 'zomato' already exists.")
    #         conn.commit()
    #     except Exception as e:
    #         logger.error(f"Error creating database: {e}")
    #         raise
    #     finally:
    #         cursor.close()
    #         conn.close()

    @task()
    def load_data_to_postgres(data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        table_customer = 'customer_record'
        table_restaurant = 'restaurant_data'
        table_order = 'order_data'

        try:
            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            # Create Customer Table
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_customer} (
                Customer_ID VARCHAR(20) PRIMARY KEY,
                Customer_Location VARCHAR(100),
                Customer_Age_Group TEXT,
                Customer_Rating FLOAT,
                Customer_Name VARCHAR(100)
            );
            """)

            # Insert Customer Data
            for customer in data['customers']:
                cursor.execute(f"""
                INSERT INTO {table_customer} (Customer_ID, Customer_Location, Customer_Age_Group, Customer_Rating, Customer_Name)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (Customer_ID) DO NOTHING;
                """, (
                    customer['Customer_ID'],
                    customer['Customer_Location'],
                    customer['Customer_Age_Group'],
                    customer['Customer_Rating'],
                    customer['Customer_Name']
                ))

            # Create Restaurant Table
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_restaurant} (
                Restaurant_ID VARCHAR(20) PRIMARY KEY,
                Name VARCHAR(100),  
                Location VARCHAR(100),  
                Cuisine_Types VARCHAR(100),
                Avg_Cost_for_Two INT,
                Ratings FLOAT,
                Reviews_Count INT,
                Operational_Hours VARCHAR(100)
            );
            """)

            # Insert Restaurant Data
            for restaurant in data['restaurants']:
                cursor.execute(f"""
                INSERT INTO {table_restaurant} (Restaurant_ID, Name, Location, Cuisine_Types, Avg_Cost_for_Two, Ratings, Reviews_Count, Operational_Hours)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (Restaurant_ID) DO NOTHING;
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

            # Create Order Table
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_order} (
                Order_ID VARCHAR(20) PRIMARY KEY,
                Restaurant_ID VARCHAR(20),
                Order_Date DATE,
                Expected_Delivery_Time INT,
                Actual_Delivery_Time INT,
                Total_Amount FLOAT,
                Order_Status VARCHAR(50),
                Payment_Method VARCHAR(50),
                Dish_Name VARCHAR(100),
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
                ON CONFLICT (Order_ID) DO NOTHING;
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

            conn.commit()
            logger.info("Data loaded into PostgreSQL successfully.")
        except Exception as e:
            logger.error(f"Error loading data into PostgreSQL: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # Task dependencies
    data = extract_data()
    create_database_task = create_database()
    create_database_task >> load_data_to_postgres(data)
