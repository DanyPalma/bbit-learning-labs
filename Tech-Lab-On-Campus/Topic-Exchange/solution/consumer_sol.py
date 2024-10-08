import pika
import os

from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_keys: str, exchange_name: str, queue_name: str) -> None:
        self.binding_keys = binding_keys
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        
        self.setupRMQConnection()
    
    
    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)
        # Establish Channel
        self.channel = connection.channel()

        # Create Queue if not already present
        self.createQueue(self.queue_name)
        # Create the exchange if not already present
        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic')
        # Bind Binding Key to Queue on the exchange
        for exchange_name in self.binding_keys:
            self.bindQueueToExchange(self.queue_name, exchange_name)
    
    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue=queueName,
            routing_key=topic,
            exchange=self.exchange_name,
        )


    def createQueue(self, queueName: str) -> None:
        # Create Queue if not already present
        self.channel.queue_declare(queue=queueName)
        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            queueName, self.on_message_callback  , auto_ack=False
        )
        

    
    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag, False)

        #Print message (The message is contained in the body parameter variable)
        print(body)
    
    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")
        # Start consuming messages
        self.channel.start_consuming() 

        
        
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        # Close Channel
        self.channel.close()
        # Close Connection
        self.connection.close()
        
