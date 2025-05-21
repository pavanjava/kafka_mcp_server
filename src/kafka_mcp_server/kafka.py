import logging
import json
import uuid
from datetime import datetime

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin.config_resource import ConfigResource
from confluent_kafka.admin import RESOURCE_BROKER, RESOURCE_TOPIC, ConfigResource, NewTopic, AdminClient

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
        # Initialize admin client for topic management and cluster health
        self.admin_client = AdminClient({'bootstrap.servers': kafka_bootstrap_url})

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

    #########################
    # Topic Management APIs
    #########################

    def create_topic(self, topic_name, num_partitions=1, replication_factor=1, config=None):
        """
        Create a new Kafka topic.

        Args:
            topic_name: Name of the topic to create
            num_partitions: Number of partitions for the topic
            replication_factor: Replication factor for the topic
            config: Additional topic configuration parameters

        Returns:
            Dict with operation result
        """
        topic_config = config or {}

        # Create new topic
        new_topics = [NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=topic_config
        )]

        # Create the topic
        try:
            futures = self.admin_client.create_topics(new_topics)

            # Wait for operation to complete
            for topic, future in futures.items():
                future.result()  # Raises exception on failure
                logger.info(f"Topic {topic} created successfully")

            return {"success": True, "message": f"Topic {topic_name} created successfully"}
        except Exception as e:
            error_msg = f"Failed to create topic {topic_name}: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

    def delete_topic(self, topic_name):
        """
        Delete a Kafka topic.

        Args:
            topic_name: Name of the topic to delete

        Returns:
            Dict with operation result
        """
        try:
            futures = self.admin_client.delete_topics([topic_name])

            # Wait for operation to complete
            for topic, future in futures.items():
                future.result()  # Raises exception on failure
                logger.info(f"Topic {topic} deleted successfully")

            return {"success": True, "message": f"Topic {topic_name} deleted successfully"}
        except Exception as e:
            error_msg = f"Failed to delete topic {topic_name}: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

    def list_topics(self):
        """
        List all topics in the Kafka cluster.

        Returns:
            List of topic names
        """
        try:
            # Get metadata for all topics
            metadata = self.admin_client.list_topics(timeout=10.0)
            topics = list(metadata.topics.keys())
            logger.info(f"Found {len(topics)} topics")
            return topics
        except Exception as e:
            error_msg = f"Failed to list topics: {str(e)}"
            logger.error(error_msg)
            raise

    def get_topic_config(self, topic_name):
        """
        Get configuration for a specific topic.

        Args:
            topic_name: Name of the topic

        Returns:
            Dict containing topic configuration
        """
        try:
            resource = ConfigResource(RESOURCE_TOPIC, topic_name)
            futures = self.admin_client.describe_configs([resource])

            # Wait for operation to complete
            configs = {}
            for res, future in futures.items():
                config = future.result()
                for key, value in config.items():
                    configs[key] = value.value

            return configs
        except Exception as e:
            error_msg = f"Failed to get config for topic {topic_name}: {str(e)}"
            logger.error(error_msg)
            raise

    def update_topic_config(self, topic_name, config_updates):
        """
        Update configuration for a specific topic.

        Args:
            topic_name: Name of the topic
            config_updates: Dict of config key-value pairs to update

        Returns:
            Dict with operation result
        """
        try:
            resource = ConfigResource(RESOURCE_TOPIC, topic_name)

            # Set the specified config values
            for key, value in config_updates.items():
                resource.set_config(key, value)

            # Apply the configuration update
            futures = self.admin_client.alter_configs([resource])

            # Wait for operation to complete
            for res, future in futures.items():
                future.result()  # Raises exception on failure

            logger.info(f"Topic {topic_name} configuration updated successfully")
            return {"success": True, "message": f"Topic {topic_name} configuration updated successfully"}
        except Exception as e:
            error_msg = f"Failed to update config for topic {topic_name}: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

    #########################
    # Cluster Management APIs
    #########################

    def get_cluster_metadata(self):
        """
        Get metadata about the Kafka cluster.

        Returns:
            Dict containing cluster metadata
        """
        try:
            # Get cluster metadata
            metadata = self.admin_client.list_topics(timeout=10.0)

            # Extract and format broker info
            brokers = []
            for broker_id, broker in metadata.brokers.items():
                brokers.append({
                    "id": broker_id,
                    "host": broker.host,
                    "port": broker.port
                })

            # Get topic info
            topics = {}
            for topic_name, topic_metadata in metadata.topics.items():
                partitions = []
                for partition_id, partition in topic_metadata.partitions.items():
                    partitions.append({
                        "id": partition_id,
                        "leader": partition.leader,
                        "replicas": partition.replicas,
                        "isrs": partition.isrs  # In-sync replicas
                    })

                topics[topic_name] = {
                    "partitions": partitions,
                    "partition_count": len(partitions)
                }

            # Combine into cluster metadata
            cluster_metadata = {
                "cluster_id": metadata.cluster_id,
                "controller_id": metadata.controller_id,
                "broker_count": len(brokers),
                "brokers": brokers,
                "topic_count": len(topics),
                "topics": topics
            }

            return cluster_metadata
        except Exception as e:
            error_msg = f"Failed to get cluster metadata: {str(e)}"
            logger.error(error_msg)
            raise

    def check_cluster_health(self):
        """
        Check the health of the Kafka cluster.

        Returns:
            Dict with health status and details
        """
        try:
            metadata = self.get_cluster_metadata()

            # Extract broker and topic info
            broker_count = metadata["broker_count"]
            topics = metadata["topics"]

            # Check if we have at least one broker
            if broker_count == 0:
                return {
                    "status": "critical",
                    "message": "No brokers available in the cluster",
                    "details": metadata
                }

            # Check for topics with under-replicated partitions
            under_replicated_topics = []
            for topic_name, topic_info in topics.items():
                for partition in topic_info["partitions"]:
                    if len(partition["isrs"]) < len(partition["replicas"]):
                        under_replicated_topics.append(topic_name)
                        break

            # Determine overall health status
            if under_replicated_topics:
                status = "warning"
                message = f"Cluster has {len(under_replicated_topics)} topics with under-replicated partitions"
            else:
                status = "healthy"
                message = f"Cluster is healthy with {broker_count} brokers and {len(topics)} topics"

            return {
                "status": status,
                "message": message,
                "broker_count": broker_count,
                "topic_count": len(topics),
                "under_replicated_topics": under_replicated_topics,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            error_msg = f"Failed to check cluster health: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "unknown",
                "message": error_msg,
                "timestamp": datetime.datetime.now().isoformat()
            }