import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from mcp.server import Server
from mcp.server.fastmcp import Context, FastMCP
from kafka import KafkaConnector
from settings import KafkaSettings, ToolSettings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def server_lifespan(server: Server) -> AsyncIterator[dict]:
    """
    Context manager to handle the lifespan of the server.
    This is used to configure the kafka connector.
    All the configuration is now loaded from the environment variables.
    Settings handle that for us.
    """
    try:
        kafka_configurations = KafkaSettings()

        logger.info(
            f"Connecting to kafka at {kafka_configurations.get_kafka_bootstrap_server()}"
        )

        kafka_connector = KafkaConnector(kafka_bootstrap_url=kafka_configurations.bootstrap_server,
                                         topic_name=kafka_configurations.topic_name,
                                         group_id=kafka_configurations.group_id)

        await kafka_connector.create_producer()

        yield {
            "kafka_connector": kafka_connector,
        }
    except Exception as e:
        logger.error(e)
        raise e
    finally:
        pass


# FastMCP is an alternative interface for declaring the capabilities
# of the server. Its API is based on FastAPI.
mcp = FastMCP("mcp-server-kafka", lifespan=server_lifespan)

# Load the tool settings from the env variables, if they are set,
# or use the default values otherwise.
tool_settings = ToolSettings()

@mcp.tool(name="kafka-publish", description=tool_settings.tool_publish_description)
async def publish(ctx: Context, information: Any) -> str:
    """
    :param ctx:
    :param information:
    :return:
    """
    await ctx.debug(f"Storing information {information} in kafka topic")
    kafka_connector: KafkaConnector = ctx.request_context.lifespan_context[
        "kafka_connector"
    ]
    await kafka_connector.publish(value=information)
    return f"published: {information}"


@mcp.tool(name="kafka-consume", description=tool_settings.tool_consume_description)
async def consumer(ctx: Context) -> str:
    """
    :param ctx:
    :param information:
    :return:
    """
    await ctx.debug(f"consuming information from kafka")
    kafka_connector: KafkaConnector = ctx.request_context.lifespan_context[
        "kafka_connector"
    ]
    information = await kafka_connector.consume()
    return f"consumed: {information}"

@mcp.tool(name="create-topic", description=tool_settings.tool_create_topic_description)
async  def create_topic(ctx: Context, information: Any):
    await ctx.debug(f"Creating topic with {information}")
    kafka_connector: KafkaConnector = ctx.request_context.lifespan_context[
        "kafka_connector"
    ]
    kafka_connector.create_topic(topic_name=information)
    return f"topic: {information} created"

@mcp.tool(name="delete-topic", description=tool_settings.tool_delete_topic_description)
async def delete_topic(ctx: Context, information: Any):
    await ctx.debug(f"Deleting topic: {information}")
    kafka_connector: KafkaConnector = ctx.request_context.lifespan_context[
        "kafka_connector"
    ]
    kafka_connector.delete_topic(topic_name=information)
    return f"topic: {information} deleted"

@mcp.tool(name="list-topics", description=tool_settings.tool_list_topic_description)
async def list_topics(ctx: Context):
    await ctx.debug(f"fetching the list of topics")
    kafka_connector: KafkaConnector = ctx.request_context.lifespan_context[
        "kafka_connector"
    ]
    response = kafka_connector.list_topics()
    return f"topics available: {response}"

@mcp.tool(name="topic-config", description=tool_settings.tool_topic_config_description)
async def topic_config(ctx: Context, information: Any):
    await ctx.debug(f"fetching the topic configuration")
    kafka_connector: KafkaConnector = ctx.request_context.lifespan_context[
        "kafka_connector"
    ]
    response = kafka_connector.get_topic_config(topic_name=information)
    return f"topic config: {response}"

@mcp.tool(name="cluster-health", description=tool_settings.tool_cluster_health_description)
async def cluster_health(ctx: Context):
    await ctx.debug(f"fetching the cluster health")
    kafka_connector: KafkaConnector = ctx.request_context.lifespan_context[
        "kafka_connector"
    ]
    response = kafka_connector.check_cluster_health()
    return f"cluster health: {response}"

@mcp.tool(name="cluster-metadata", description=tool_settings.tool_cluster_metadata_description)
async def cluster_metadata(ctx: Context):
    await ctx.debug(f"fetching the cluster metadata")
    kafka_connector: KafkaConnector = ctx.request_context.lifespan_context[
        "kafka_connector"
    ]
    response = kafka_connector.get_cluster_metadata()
    return f"cluster metadata: {response}"