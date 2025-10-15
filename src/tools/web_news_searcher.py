# Databricks notebook source
# !pip install uv
# !uv add langchain-tavily tavily-python --active  --quiet
# !uv sync --active --quiet
# dbutils.library.restartPython()

# COMMAND ----------

import getpass
import os
from dotenv import load_dotenv
from langchain_core.tools import BaseTool, tool
from langchain_tavily import TavilySearch
from tavily import TavilyClient
from langchain.tools import Tool
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional, Type, List

# COMMAND ----------

load_dotenv("../../.env")

# COMMAND ----------

class TavilyQuerySchema(BaseModel):
    """Schema defining the arguments for TavilyTool."""
    query: str = Field(..., description="The search query to use")
    max_results: Optional[int] = None
    days: Optional[int] = None

# COMMAND ----------

class TavilyTool(BaseTool):
    """
    Custom LangChain Tool that integrates with the Tavily API.
    
    - Performs a web search on news (focused on Brazil and SRAG context).
    - Extracts detailed content from the retrieved sources.
    - Returns structured results with title, summary snippet (context), and full extracted content.
    """

    name: str = "tavily_search_tool"
    description: str = (
        "Invoke Tavily search. Accepts a schema with fields: query, "
        "max_results (optinal: default =5), days (optional : default=30). Returns search results."
    )
    args_schema: Type[BaseModel] = TavilyQuerySchema
    
    def _search_web_news(self, query: str, max_results: int = 5, days: Optional[int] = 30) -> Dict:
        """
        Run a Tavily search query and return results.
        
        Args:
            query (str): The search query from the agent.
            max_results (int): Max number of search results.
            days (int): Time window (days) for filtering recent results.
            
        Returns:
            TavilyClient, Dict: client instance and search response.
        """
         # Add default search query to improve query results.
        default_query = "Como está o cenário atual de casos de Sindrome Respiratória Aguda Grave no Brasil? " 
        search_query = default_query + query
        tavily_api_key = os.environ.get("TAVILY_API_KEY")
        tavily_client = TavilyClient(api_key=tavily_api_key)
        # Instantiate a TavilySearch client
        response = tavily_client.search(
            query=search_query,
            max_results=max_results,
            search_depth="basic",
            days=days,
            country="brazil",
        )
        return tavily_client, response

    def _extract_results(self, tavily_client, response):
        """
        Extract full page content from search results and keep original summaries.
        
        Args:
            tavily_client (TavilyClient): client to call extract.
            response (Dict): search response containing initial results.
            
        Returns:
            List[Dict]: Enriched search results with context + extracted content.
        """
        topic = response["query"]
        context =[]
        context.append({
            "topic": topic,
            "sources": [
                { "url": result["url"], 
                "title": result["title"],
                "context": result["content"]} for result in response["results"]
            ]
        })

        extracted_results = []

        for topic in context:
            extract_response = tavily_client.extract([source["url"] for source in topic["sources"]])

        for extracted_result in extract_response["results"]:
            for source in topic["sources"]:
                if source["url"] == extracted_result["url"]:
                    for source_context in context[0]["sources"]:
                        if source["url"] == source_context["url"]:
                            source["context"] = source_context["context"]
                source["content"] = extracted_result["raw_content"]

        for extracted_result in extract_response["failed_results"]:
            for source in topic["sources"]:
                if source["url"] == extracted_result["url"]:
                    topic["sources"].remove(source)

        extracted_results.append(topic)
        return extracted_results

    def _run(self, query: str, max_results: int = 5, days: int = 30) -> List[Dict]:
        """
        Run the TavilyTool: search + extract.
        
        Args:
            query (str): The search query.
            max_results (int): Number of results to return.
            days (int): How recent the results should be.
        
        Returns:
            List[Dict]: Enriched structured results.
        """
        tavily_client, response =  self._search_web_news(query=query, max_results=max_results, days=days)
        return self._extract_results(tavily_client, response)
       

# COMMAND ----------

# tavily_api_key = os.environ.get("TAVILY_API_KEY")

# tavily_client = TavilyClient(api_key=tavily_api_key)

# COMMAND ----------

# search_query = "Como está o cenário atual de casos de Sindrome Respiratória Aguda Grave no Brasil?" 

# COMMAND ----------

# resp = tavily_client.search(
#     query=search_query,
#     max_results=3,
#     # topic="news",
#     # include_answer=True,
#     # include_raw_content=False,
#     # include_images=False,
#     # include_image_descriptions=False,
#     search_depth="basic",
#     time_range="month",
#     # include_domains=None,
#     # exclude_domains=None,
#     country="brazil",
# )

# COMMAND ----------

# resp

# COMMAND ----------

# resp["results"]

# COMMAND ----------

# topic = resp["query"]
# context =[]
# context.append({
#       "topic": topic,
#       "sources": [
#           { "url": result["url"], 
#            "title": result["title"],
#            "context": result["content"]} for result in resp["results"]
#       ]
#   })

# extracted_results = []

# for topic in context:
#   extract_response = tavily_client.extract([source["url"] for source in topic["sources"]])

#   for extracted_result in extract_response["results"]:
#     for source in topic["sources"]:
#       if source["url"] == extracted_result["url"]:
#         for source_context in context[0]["sources"]:
#           if source["url"] == source_context["url"]:
#             source["context"] = source_context["context"]
#         source["content"] = extracted_result["raw_content"]

#   for extracted_result in extract_response["failed_results"]:
#     for source in topic["sources"]:
#       if source["url"] == extracted_result["url"]:
#         topic["sources"].remove(source)

#   extracted_results.append(topic)


# COMMAND ----------

# extracted_results
