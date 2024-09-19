import pika
import os
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        # Save parameters to class variables
        self.routing_key = routing_key
        self.exchange_name = exchange_name

        # Call setupRMQConnection
        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create the topic exchange if not already present
        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="topic")
        
        self.publishOrder


    def publishOrder(self, message: str) -> None:
        # Create Appropiate Topic String
        

        # Send serialized message or String
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message,
        )

        # Print Confirmation

        # Close channel and connection
        self.channel.close()
        self.connection.close()