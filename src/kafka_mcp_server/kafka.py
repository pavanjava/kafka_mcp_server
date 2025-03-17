import logging
import json
import uuid

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaConnector:
    """
    Encapsulates the connection to a kafka server and all the methods to interact with it.
    :param kafka_bootstrap_url: The URL of the kafka server.
    :param topic_name: The topic to which the client will talk to.
    """

    def __init__(self, kafka_bootstrap_url: str, topic_name: str, group_id: str):
        self.KAFKA_BOOTSTRAP_SERVERS = kafka_bootstrap_url
        self.topic_name = topic_name
        self.group_id = group_id
        self.producer = None

    async def create_producer(self):
        """Create and start a Kafka producer."""
        producer = AIOKafkaProducer(bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        logger.info(f"Kafka producer started, connected to {self.KAFKA_BOOTSTRAP_SERVERS}")
        self.producer = producer
        return producer

    async def close_producer(self):
        """Close the Kafka producer."""
        await self.producer.stop()
        logger.info("Kafka producer stopped")

    async def publish(self, value):
        """
        Publish a message to the specified Kafka topic.

        Args:
            producer: AIOKafkaProducer instance
            topic_name: Topic to publish to
            key: Message key (can be None)
            value: Message value
        """
        try:
            key = str(uuid.uuid4())
            # Convert value to bytes if it's not already
            if isinstance(value, dict):
                value_bytes = json.dumps(value).encode('utf-8')
            elif isinstance(value, str):
                value_bytes = value.encode('utf-8')
            else:
                value_bytes = value

            # Convert key to bytes if it's not None and not already bytes
            key_bytes = None
            if key is not None:
                if isinstance(key, str):
                    key_bytes = key.encode('utf-8')
                else:
                    key_bytes = key

            # Send message
            await self.producer.send_and_wait(self.topic_name, value=value_bytes, key=key_bytes)
            logger.info(f"Published message with key {key} to topic {self.topic_name}")
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            raise

    async def consume(self, from_beginning=True):
        """
        Consume messages from the specified Kafka topics.

        Args:
            from_beginning: Whether to start consuming from the beginning
        """
        # Convert single topic to list
        if isinstance(self.topic_name, str):
            topics = [self.topic_name]

        # Set auto_offset_reset based on from_beginning
        auto_offset_reset = 'earliest' if from_beginning else 'latest'

        # Create consumer
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
        )

        # Start consumer
        await consumer.start()
        logger.info(f"Kafka consumer started, subscribed to {topics}")

        messages = []

        try:
            # Get a batch of messages with timeout
            batch = await consumer.getmany(timeout_ms=5000)

            for tp, msgs in batch.items():
                for msg in msgs:
                    logger.info(f"Raw message received: {msg}")
                    processed_message = await self._process_message(msg)
                    messages.append(processed_message)

            return messages
        finally:
            # Close consumer
            await consumer.stop()
            logger.info("Kafka consumer stopped")

    async def _process_message(self, msg):
        """
       Process a message received from Kafka.

       Args:
           msg: Message object from Kafka
       """
        try:
            # Decode the message value
            if msg.value:
                try:
                    value = json.loads(msg.value.decode('utf-8'))
                except json.JSONDecodeError:
                    value = msg.value.decode('utf-8')
            else:
                value = None

            # Decode the message key
            key = msg.key.decode('utf-8') if msg.key else None

            logger.info(f"Received message: Topic={msg.topic}, Partition={msg.partition}, "
                        f"Offset={msg.offset}, Key={key}, Value={value}")

            # Your message processing logic here
            return value
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            raise