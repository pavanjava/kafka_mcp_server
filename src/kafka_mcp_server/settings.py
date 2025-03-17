from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional

DEFAULT_TOOL_PUBLISH_DESCRIPTION = (
    "publish the information to the kafka topic for the down stream usage."
)
DEFAULT_TOOL_CONSUME_DESCRIPTION = (
    "Look up topics in kafka. Use this tool when you need to: \n"
    " - consume information from the topics\n"
)


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