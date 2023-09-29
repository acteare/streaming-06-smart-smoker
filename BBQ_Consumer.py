"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

    Author: Amelia Teare
    Date: September 29, 2023

"""

import pika
import sys
import time
import logging
from collections import deque
import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


SMOKER_TEMP = deque(maxlen=5)
FOOD_TEMP = deque(maxlen=20)

SMOKER_ALERT_THRESHOLD = 15.0
FOOD_STALL_THRESHOLD = 1.0

# define a callback function to be called when a message is received
def process_temperature(body, temp_name, temp_deque, alert_threshold):
    """ Reads the temperature and alerts the user of temperature changes
    
    Parameters:
        Body
        temp_name
        temp_deque
        alert_threshold
    
    """
    try:
        timestamp_str, temperature_str = body.decode().split(',')
        timestamp = datetime.datetime.strptime(timestamp_str, '%m/%d/%y %H:%M:%S')
        temperature = float(temperature_str.strip()) if temperature_str.strip().lower() != 'none' else None
        temp_deque.append((timestamp, temperature))

        if len(temp_deque) == temp_deque.maxlen:
            valid_temps = [temp[1] for temp in temp_deque if temp[1] is not None]

            if len(valid_temps) < 2:
                logging.warning(f"Not enough valid temperatures for {temp_name} callback.")
                return
        
            first_temp = valid_temps[0]
            last_temp = valid_temps[-1]
            time_difference = (temp_deque[-1][0] - temp_deque[0][0]).total_seconds() / 60.0

            logging.info(f'{temp_name} - First Temp: {first_temp}, Last Temp: {last_temp}, Time Difference: {time_difference} minutes')
            if abs(first_temp - last_temp) >= alert_threshold and time_difference <= get_time_window(temp_name):
                logging.info(f'{temp_name} Alert: Temperature change >= {alert_threshold}°F in {time_difference} minutes.')

    except Exception as e:
        logging.error(f'Error in {temp_name} callback: {e}')

def get_time_window(temp_name):

    if temp_name == "Smoker":
        return 2.5  # Smoker time window is 2.5 minutes
    elif temp_name == "Food A":
        return 10.0   # Food time window is 10 minutes
    elif temp_name == "Food B":
        return 10.0   # Food time window is 10 minutes
    else:
        return None

# define a callback function to be called when a message is received
def smoker_callback(ch, method, properties, body):
    process_temperature(body, "Smoker", SMOKER_TEMP, SMOKER_ALERT_THRESHOLD)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def food_a_callback(ch, method, properties, body):
    process_temperature(body, "Food A", FOOD_TEMP, FOOD_STALL_THRESHOLD)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def food_b_callback(ch, method, properties, body):
    process_temperature(body, "Food B", FOOD_TEMP, FOOD_STALL_THRESHOLD)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a main function to run the program
def main(hn: str = "localhost"):
    
    try:
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

        # use the connection to create a communication channel
        channel = connection.channel()

        channel.queue_declare("01-smoker", durable=True)
        channel.queue_declare("02-food-A", durable=True)
        channel.queue_declare("03-food-B", durable=True)

        channel.basic_qos(prefetch_count=1)

        channel.basic_consume("01-smoker", smoker_callback, auto_ack=False)
        channel.basic_consume("02-food-A", food_a_callback, auto_ack=False)
        channel.basic_consume("03-food-B", food_b_callback, auto_ack=False)

        # log a message for the user
        logging.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logging.info("")
        logging.error("ERROR: Something went wrong.")
        logging.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("")
        logging.info("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logging.info("\nClosing connection. Goodbye.\n")
        connection.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost")
