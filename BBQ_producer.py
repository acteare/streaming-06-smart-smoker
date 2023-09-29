"""
    This program sends temperature reading data from a smoker to a queue on the RabbitMQ server. 

    Author: Amelia Teare
    Date: September 21, 2023

"""

import pika
import sys
import webbrowser
import csv
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

HOST = "localhost"
PORT = 9999
ADDRESS_TUPLE = (HOST, PORT)
FILE_NAME_TASKS = "smoker-temps.csv"
SHOW_OFFER = True # By selecting True, you are ensuring that the admin website automatically opens. To turn this feature off, type "FALSE"

def rabbit_admin():
    """Offer to open the RabbitMQ Admin website"""
    global SHOW_OFFER
    if SHOW_OFFER:
        webbrowser.open_new("http://localhost:15672/#/queues")


def send_reading(channel, queue_name, timestamp, temperature):
    """
    Creates and sends a temperature reading to the queue each execution.
    This process runs and finishes.

    Parameters:
        host: the host name or IP address of the RabbitMQ server
        queue_name: the name of the queue
        timestamp: the time at which the temperature reading was taken
        temperature: the temperature reading to be sent to the queue
    """
def send_reading(channel, queue_name, timestamp, temperature):
    channel.basic_publish(exchange="", routing_key=queue_name, body=f"{timestamp},{temperature}")
    logging.info(f"Sent to {queue_name} Queue: Timestamp={timestamp}, Temperature={temperature}")

def read_from_file(file_name):
    conn = pika.BlockingConnection(pika.ConnectionParameters(HOST))
    ch = conn.channel()
    with open(file_name, "r", newline="") as input_file:
            reader = csv.reader(input_file)
            next(reader)
                
            for row in reader:
                timestamp = row[0]
                smoker_temp = float(row[1]) if row [1] else None
                food_a_temp = float(row[2]) if row [2] else None
                food_b_temp = float(row[3]) if row [3] else None

                send_reading(ch, "01-smoker", timestamp, smoker_temp)
                send_reading(ch, "02-food-A", timestamp, food_a_temp)
                send_reading(ch, "03-food-B", timestamp, food_b_temp)

                time.sleep(30)

                logging.info(f"Sent: Timestamp={timestamp}, Smoker Temp={smoker_temp}, Food A Temp={food_a_temp}, Food B Temp={food_b_temp}")
                logging.info(" Type CTRL+C to cancel the program  ")
  

def main():
    """
    This function will be used to do the following:
    1. connect to RabbitMQ
    2. Get a communication channel
    3. Use the channel to queue_delete() all 3 queues
    4. Use the channel to queue_declare() all 3 queues
    5. Open the file, get your csv reader, for each row, use the channel to basic_publish() a message.
    """
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(HOST))
        
        # use the connection to create a communication channel
        ch = conn.channel()

        # delete the existing queues using queue_delete()
        ch.queue_delete(queue="01-smoker")
        ch.queue_delete(queue="02-food-A")
        ch.queue_delete(queue="03-food-B")

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue="01-smoker", durable=True)
        ch.queue_declare(queue="02-food-A", durable=True)
        ch.queue_declare(queue="03-food-B", durable=True)

        read_from_file(FILE_NAME_TASKS)        

    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()



# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    rabbit_admin()
   
    # get the tasks from the csv file using the custom function
    main()