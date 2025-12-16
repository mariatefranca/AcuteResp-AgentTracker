from typing import Any, Dict, List
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import BaseMessage
from langchain_core.outputs import LLMResult
from langchain_core.prompts import ChatPromptTemplate

class LoggingHandler(BaseCallbackHandler):
    """
    LoggingHandler is a callback handler for logging events during the execution of
    chat models, chains, and tools in LangChain workflows.

    Methods:
        on_chat_model_start: Logs when a chat model starts.
        on_llm_end: Logs when a chat model ends and outputs the response.
        on_chain_start: Logs when a chain starts, including its name.
        on_chain_end: Logs when a chain ends and outputs the results.
        on_tool_start: Logs when a tool starts, including its name.
        on_tool_end: Logs when a tool ends and outputs the result.
        on_tool_error: Logs any errors encountered during tool execution.
    """

    def on_chat_model_start(
        self, serialized: Dict[str, Any], messages: List[List[BaseMessage]], **kwargs
    ) -> None:
        print("Chat model started")

    def on_llm_end(self, response: LLMResult, **kwargs) -> None:
        print(f"Chat model ended, response: {response}")

    def on_chain_start(
        self, serialized: Dict[str, Any], inputs: Dict[str, Any], **kwargs
    ) -> None:
        print(f"Chain {serialized.get('name')} started")

    def on_chain_end(self, outputs: Dict[str, Any], **kwargs) -> None:
        print(f"Chain ended, outputs: {outputs}")

    def on_tool_start(
        self, serialized: Dict[str, Any], input_str: str, **kwargs
    ) -> None:
        print(f"Tool {serialized.get('name')} started")

    def on_tool_end(self, output: str, **kwargs) -> None:
        print(f"Tool ended, output: {output}")

    def on_tool_error(self, error: Exception, **kwargs) -> None:
        print(f"Tool error: {error}")