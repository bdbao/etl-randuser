# import uuid
# import json
# import time
# import logging
# import requests
# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from kafka import KafkaProducer

# default_args = {
#     'owner': 'airscholar',
#     'start_date': datetime(2025, 1, 1, 10, 00)
# }

# def get_data():
#     """Fetch random user data from API"""
#     res = requests.get("https://randomuser.me/api/")
#     res = res.json()
#     return res['results'][0]

# def format_data(res):
#     """Format data for Kafka"""
#     location = res['location']
#     return {
#         'id': str(uuid.uuid4()),  # Convert UUID to string
#         'first_name': res['name']['first'],
#         'last_name': res['name']['last'],
#         'gender': res['gender'],
#         'address': f"{location['street']['number']} {location['street']['name']}, "
#                    f"{location['city']}, {location['state']}, {location['country']}",
#         'post_code': location['postcode'],
#         'email': res['email'],
#         'username': res['login']['username'],
#         'dob': res['dob']['date'],
#         'registered_date': res['registered']['date'],
#         'phone': res['phone'],
#         'picture': res['picture']['medium']
#     }

# def stream_data():
#     """Fetch and send data to Kafka"""
#     logging.info("Initializing Kafka producer...")
#     producer = KafkaProducer(
#         bootstrap_servers=['broker:29092'],
#         value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Ensure JSON serialization
#     )
    
#     start_time = time.time()
    
#     while time.time() - start_time < 60:  # Run for 1 minute
#         try:
#             res = get_data()
#             formatted_data = format_data(res)
#             producer.send('users_created', formatted_data)
#             logging.info(f"Sent: {formatted_data}")
#         except Exception as e:
#             logging.exception("Error while streaming data")
    
#     producer.flush()  # Ensure all messages are sent
#     logging.info("Streaming completed.")

# # Define DAG
# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:

#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )


import uuid
import json
import time
import logging
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2025, 1, 15)
}

def get_data():
    """Fetch and return formatted user data from API."""
    try:
        res = requests.get("https://randomuser.me/api/").json()
        user = res['results'][0]
        location = user['location']
        return {
            'id': str(uuid.uuid4()),
            'first_name': user['name']['first'],
            'last_name': user['name']['last'],
            'gender': user['gender'],
            'address': f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}",
            'post_code': location['postcode'],
            'email': user['email'],
            'username': user['login']['username'],
            'dob': user['dob']['date'],
            'registered_date': user['registered']['date'],
            'phone': user['phone'],
            'picture': user['picture']['medium']
        }
    except (requests.RequestException, KeyError) as e:
        logging.error(f"Error fetching data: {e}")
        return None

def stream_data():
    """Fetch and send data to Kafka."""
    logging.info("Initializing Kafka producer...")
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    end_time = time.time() + 60  # Run for 1 minute
    while time.time() < end_time:
        data = get_data()
        if data:
            producer.send('users_created', data)
            logging.info(f"Sent: {data}")
    
    producer.flush()
    logging.info("Streaming completed.")

with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
