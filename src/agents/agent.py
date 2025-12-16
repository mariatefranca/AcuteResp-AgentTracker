import os
import mlflow
import toml
import sys
import pyspark.sql.functions as F
from databricks_langchain import ChatDatabricks
import datetime
from typing import Any, Generator, Optional, Sequence, Union
from databricks_langchain import ChatDatabricks
from langchain_core.tools import BaseTool, tool
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.language_models import LanguageModelLike
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse, ChatAgentChunk, ChatContext
from mlflow.types.llm import ChatCompletionResponse, ChatChoice, ChatMessage, ChatCompletionChunk, ChatChunkChoice, ChatChoiceDelta
from langchain_core.tools import StructuredTool
from langgraph.graph.message import add_messages
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
    convert_to_openai_messages,
)
from pydantic import BaseModel, create_model
from typing import Annotated, TypedDict

try:
    from IPython.display import Image, display
except ImportError:
    pass

sys.path.append("../../src")

from tools.report_finder import ReportSearchResult, FindTodaySRAGReportTool
from tools.metrics_calculator import SRAGMetrics, SRAGMetricsOutput
from tools.visual_generator import SRAGVisualization, SRAGPlotsOutput
from tools.database_searcher import SparkSQLQuerySchema, SparkSQLQueryTool
from tools.web_news_seacher import TavilyTool, TavilyQuerySchema
from tools.report_assembler import GenerateSRAGReportTool, AgentNarrativeInput
from agent_config.callback_handler import LoggingHandler
from agent_config.prompt import system_prompt


env_vars = toml.load("../../conf/env_vars.toml")

# Set as environment variables.
for key, value in env_vars.items():
    os.environ[key] = str(value)

callbacks = [LoggingHandler()]

LLM_ENDPOINT_NAME = env_vars["LLM_ENDPOINT_NAME"]
llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME, callbacks=callbacks)

# Instantiate all tools.
srag_report_finder_tool = FindTodaySRAGReportTool()
database_searcher_tool = SparkSQLQueryTool()
web_searcher_tool = TavilyTool()
report_assembler_tool = GenerateSRAGReportTool()

# Create the metric calculator StructuredTool.
srag_metric_calculator_tool = StructuredTool.from_function(
    func=SRAGMetrics().generate_report_metrics,
    name="generate_srag_report_metrics",
    description=(
        "Generates SRAG epidemiological metrics (counts, variation, ICU admission, "
        "HTML ICU tables) for report generation."
    ),
    structured_output_schema=SRAGMetricsOutput,
    return_direct=True
)

# Create the plot generator StructuredTool.
srag_plot_generator_tool = StructuredTool.from_function(
    func=SRAGVisualization().generate_plot_data,
    name="generate_srag_report_plots",
    description=(
        "Compute SRAG statistic data used for report visualization, including time series, "
        "UF choropleth, and vaccination rate charts."
    ),
    structured_output_schema=SRAGPlotsOutput,
    return_direct=True
)

tools = []
tools.append(srag_report_finder_tool)
tools.append(srag_metric_calculator_tool)
tools.append(srag_plot_generator_tool)
tools.append(database_searcher_tool)
tools.append(web_searcher_tool)
tools.append(report_assembler_tool)


#  The state for the agent workflow, including the conversation and any custom data.
class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    custom_inputs: Optional[dict[str, Any]]
    custom_outputs: Optional[dict[str, Any]]

# Define the LangGraph agent that can call tools
def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[ToolNode, Sequence[BaseTool]],
    system_prompt: Optional[str] = None,
) -> CompiledStateGraph:
    # Bind tools to the model
    model = model.bind_tools(tools)  

    # Function to check if agent should continue or finish based on last message
    def routing_logic(state: ChatAgentState):
        messages = state["messages"]
        last_message = messages[-1]
        # If function (tool) calls are present, continue; otherwise, end
        if last_message.get("tool_calls"):
            return "continue"
        else:
            return "end"

    # Preprocess: optionally prepend a system prompt to the conversation history
    if system_prompt:
        preprocessor = RunnableLambda(
            lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])

    model_runnable = preprocessor | model  # Chain the preprocessor and the model

    # The function to invoke the model within the workflow
    def call_model(
        state: ChatAgentState,
        config: RunnableConfig,
    ):
        response = model_runnable.invoke(state, config)
        return {"messages": [response]}

    workflow = StateGraph(ChatAgentState)  # Create the agent
    workflow.add_node("agent", RunnableLambda(call_model))  # Agent node (LLM)
    workflow.add_node("tools", ChatAgentToolNode(tools))            # Tools node

    workflow.set_entry_point("agent")  # Start at agent node
    workflow.add_conditional_edges(
        "agent",
        routing_logic,
        {
            "continue": "tools",  # If the model requests a tool call, move to tools node
            "end": END,           # Otherwise, end the workflow
        },
    )
    workflow.add_edge("tools", "agent")  # After tools are called, return to agent node

    # Compile and return the tool-calling agent workflow
    return workflow.compile()

    
class LangGraphAgent(ChatAgent):
    def __init__(
        self,
        agent: CompiledStateGraph):
        self.agent = agent
    def _normalize_messages(self, model_input):
        """
        Review App / Serving always sends a pandas DataFrame.
        Local tests may send dict.
        """
        if isinstance(model_input, pd.DataFrame):
            messages = model_input.iloc[0]["messages"]
        else:
            messages = model_input["messages"]

        return messages

    def predict(
        self,
        messages: Sequence[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,

    ) -> ChatAgentResponse:
        request = {"messages": self._convert_messages_to_dict(messages)}

        messages = []
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                messages.extend(
                    ChatAgentMessage(**msg) for msg in node_data.get("messages", [])

                )
        return ChatAgentResponse(messages=messages)
    
mlflow.langchain.autolog()
# Create the agent graph with an LLM, tool set, and system prompt (if given)
agent = create_tool_calling_agent(llm, tools, system_prompt)
AGENT = LangGraphAgent(agent)
mlflow.models.set_model(AGENT)

# Display the agent graph.
try:
    display(Image(agent.get_graph(xray=True).draw_mermaid_png()))
except Exception:
    # This requires some extra dependencies and is optional
    pass
