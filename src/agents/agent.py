# Databricks notebook source
# !pip install uv --quiet
# !uv sync --active --quiet
# dbutils.library.restartPython()

# COMMAND ----------

sys.append("../src")

# COMMAND ----------

# MAGIC %run ../tools/report_finder.py

# COMMAND ----------

# MAGIC %run ../tools/metric_calculator.py

# COMMAND ----------

# MAGIC %run ../tools/visual_generator.py

# COMMAND ----------

# MAGIC %run ../tools/database_searcher.py

# COMMAND ----------

# MAGIC %run ../tools/web_news_searcher.py

# COMMAND ----------

# MAGIC %run ../tools/report_assembler.py

# COMMAND ----------

# MAGIC %run ../agent_config/callback_handler.py

# COMMAND ----------

import os
import mlflow
import toml
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
from IPython.display import Image, display

# COMMAND ----------

env_vars = toml.load("../../conf/env_vars.toml")

# COMMAND ----------

callbacks = [LoggingHandler()]

LLM_ENDPOINT_NAME = env_vars["LLM_ENDPOINT_NAME"]
llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME, callbacks=callbacks)

# COMMAND ----------

system_prompt = """
"Você é um analista de dados em saúde que consulta dados do SUS (Sistema Único de Saúde do Brasil) sobre síndrome respiratória grave e retorna um relatório diário sobre a situação da doença trazendo métricas relevantes e as respectivas explicações que ajudem a explicar o cenário atual. Você utiliza vocabulário técnico em suas respostas e relatórios gerados"

Passo 1: Utilize a ferramenta srag_report_finder_tool para encontrar o relatório diário mais atualizado sobre a situação da doença e carregar as informações contidas nele.

Caso você não encontre um relatório do dia, utiliza o passo 2 para gerar um novo relatório. Se voê encontrou o relatório da data atual, siga para o passo 3.
Passo 2:
- Utiliza a ferramenta srag_metric_calculator_tool para conehcer todas as métricas epidemiológicas que são incluídas no relatório.
- Utilize a ferramenta srag_plot_generator_tool para obter informações sobre os gráficos relevantes para o relatório.
- Utilize a ferramenta web_searcher_tool para efetuar uma busca informações na internet sobre o cenário atual da Sindrome Respiratória Aguda Grave a fim de contextualizar o resultado das métricas e visualizações encontradas.
- Em seguida sumarize os resultados encontrados sobre a doença SRAG e forneça a resposta final da análise efetuada em um dict que será usado como input da ferramenta report_assembler_tool.
O dict deve conter os seguintes campos:
    - srag_description: Breve descrição de SRAG, em até 150 palavras, u.
    - comment_cases_evolution_count: Um comentário de até 150 palavras sobre a evolução do número de casos mês atual, nos ultimos meses,  de forma geral e nos diferentes estados.
    - conclusions_disease_evolution_icu_occupation: Uma conclusão de até 300 palavras sobre a evolução do número de casos, taxa de pacientes vacinados e taxa de ocupação da UTI nos últimos meses, contextualizando com notícias atuais.
- Chame a ferramenta report_assembler_tool e passe como argumento o dict com os 3 itens acima.

Passo 3:
Agora que você possui um relatório padrão de SRAG e suas informações, você pode responder perguntas do usuário sobre SRAG/doenças respiratórias.
Caso o usuário perguntar alguma métrica não incluída no relatório, use a ferramenta database_searcher_tool para buscar informações no banco de dados de SRAG (tabela srag_features). Você acessar a tabela srag_features_dictionary para obter uma descrição de cada coluna existente na tabela srag_features. Você deve preferir fazer perguntas diretas ao agente database_searcher_tool, ao invés queries de READ em SQL.
Voc6e também pode usar a ferramenta web_searcher_tool para efetuar uma nova busca informações na internet sobre o cenário atual da Sindrome Respiratória Aguda Grave. Não utilize essa ferrament mais de 2 vezes.

Mesmo que o usuário não pergunte sobre o relatório, ao final da respsota sempre chame a ferramenta report_assembler_tool para gerar/exibir o relatório final junto com a resposta solicitada. Você não deve chamar a ferramenta report_assembler_tool mais de uma vez.
"""

# COMMAND ----------

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

# COMMAND ----------

tools = []
tools.append(srag_report_finder_tool)
tools.append(srag_metric_calculator_tool)
tools.append(srag_plot_generator_tool)
tools.append(database_searcher_tool)
tools.append(web_searcher_tool)
tools.append(report_assembler_tool)

# COMMAND ----------

# MAGIC %md
# MAGIC # Define the agent logic.

# COMMAND ----------

#  The state for the agent workflow, including the conversation and any custom data
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

# COMMAND ----------

class LangGraphAgent(ChatAgent):
    def __init__(
        self,
        agent: CompiledStateGraph):
        self.agent = agent

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

# COMMAND ----------


mlflow.langchain.autolog()
# Create the agent graph with an LLM, tool set, and system prompt (if given)
agent = create_tool_calling_agent(llm, tools, system_prompt)
AGENT = LangGraphAgent(agent)
mlflow.models.set_model(AGENT)

# COMMAND ----------

# Display the agent graph.
try:
    display(Image(agent.get_graph(xray=True).draw_mermaid_png()))
except Exception:
    # This requires some extra dependencies and is optional
    pass

# COMMAND ----------

# AGENT.predict({"messages": [{"role": "user", "content": "O que é SRAG?"}]})

# COMMAND ----------

# AGENT.predict({"messages": [{"role": "user", "content": "Qual a taxa de mortalidade por SRAG esse mês no Brazil?"}]})
