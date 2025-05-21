from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional

DEFAULT_TOOL_PUBLISH_DESCRIPTION =  "publish the information to the kafka topic for the down stream usage."

DEFAULT_TOOL_CONSUME_DESCRIPTION = ("Look up topics in kafka. Use this tool when you need to: \n"
                                    " - consume information from the topics\n")

DEFAULT_TOOL_CREATE_TOPIC_DESCRIPTION = ( "Use this tool when you need to: \n"
                                          " - create the new topic\n")

DEFAULT_TOOL_DELETE_TOPIC_DESCRIPTION = ( "Look up topics in kafka. Use this tool when you need to: \n"
                                          " - delete the specific topic\n")

DEFAULT_TOOL_LIST_TOPIC_DESCRIPTION = ( "Look up topics in kafka. Use this tool when you need to: \n"
                                        " - List and display all the topics\n")

DEFAULT_TOOL_TOPIC_CONFIG_DESCRIPTION = ( "Look up topics in kafka. Use this tool when you need to: \n"
                                        " - List and display the specific topic configuration\n")

DEFAULT_TOOL_CLUSTER_HEALTH_DESCRIPTION = ( "Look up clusters in platform. Use this tool when you need to: \n"
                                          " - display the specific cluster health condition\n")

DEFAULT_TOOL_CLUSTER_METADATA_DESCRIPTION = ( "Look up clusters in platform. Use this tool when you need to: \n"
                                          " - display the cluster metadata \n")

class ToolSettings(BaseSettings):
    """
    Configuration for all the tools.
    """

    tool_publish_description: str = Field(
        default=DEFAULT_TOOL_PUBLISH_DESCRIPTION,
        validation_alias="TOOL_PUBLISH_DESCRIPTION",
    )
    tool_consume_description: str = Field(
        default=DEFAULT_TOOL_CONSUME_DESCRIPTION,
        validation_alias="TOOL_CONSUME_DESCRIPTION",
    )

    tool_create_topic_description: str = Field(
        default=DEFAULT_TOOL_CREATE_TOPIC_DESCRIPTION,
        validation_alias="TOOL_CREATE_TOPIC_DESCRIPTION",
    )

    tool_delete_topic_description: str = Field(
        default=DEFAULT_TOOL_DELETE_TOPIC_DESCRIPTION,
        validation_alias="TOOL_DELETE_TOPIC_DESCRIPTION",
    )

    tool_list_topic_description: str = Field(
        default=DEFAULT_TOOL_LIST_TOPIC_DESCRIPTION,
        validation_alias="TOOL_LIST_TOPIC_DESCRIPTION",
    )

    tool_topic_config_description: str = Field(
        default=DEFAULT_TOOL_TOPIC_CONFIG_DESCRIPTION,
        validation_alias="TOOL_TOPIC_CONFIG_DESCRIPTION",
    )

    tool_cluster_health_description: str = Field(
        default=DEFAULT_TOOL_CLUSTER_HEALTH_DESCRIPTION,
        validation_alias="TOOL_CLUSTER_HEALTH_DESCRIPTION",
    )

    tool_cluster_metadata_description: str = Field(
        default=DEFAULT_TOOL_CLUSTER_METADATA_DESCRIPTION,
        validation_alias="TOOL_CLUSTER_METADATA_DESCRIPTION",
    )

class KafkaSettings(BaseSettings):
    """
    Configuration for the Kafka connector.
    """

    bootstrap_server: Optional[str] = Field(default=None, validation_alias="KAFKA_BOOTSTRAP_SERVERS")
    topic_name: Optional[str] = Field(default=None, validation_alias="TOPIC_NAME")
    from_beginning: Optional[bool] = Field(default=False, validation_alias="IS_TOPIC_READ_FROM_BEGINNING")
    group_id: Optional[str] = Field(default="kafka-mcp-group", validation_alias="DEFAULT_GROUP_ID_FOR_CONSUMER")


    def get_kafka_bootstrap_server(self) -> str:
        """
        Get the Kafka location from bootstrap URL.
        """
        return self.bootstrap_server