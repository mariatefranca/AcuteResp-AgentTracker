# Databricks notebook source
!pip install uv
!uv add install mlflow[genai] databricks-sdk[genai] openai python-dotenv databricks-cli dotenv --active  --quiet
!uv sync --active --quiet
dbutils.library.restartPython()


# COMMAND ----------

import json
import mlflow.deployments as deployments
import os
import requests
import time
import toml
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

# COMMAND ----------

env_vars = toml.load("../../conf/env_vars.toml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Agent Context

# COMMAND ----------

# Context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST = ctx.apiUrl().get()
TOKEN = ctx.apiToken().get()

SCOPE = "openai_scope"
SECRET_KEY = "openai_api_key"

# Helpers
def scope_exists(host: str, token: str, scope_name: str) -> bool:
    """Return True if a scope called `scope_name` exists."""
    r = requests.get(f"{host}/api/2.0/secrets/scopes/list", 
    headers={"Authorization": f"Bearer {token}"},
    timeout=20)

    r.raise_for_status()
    return any(s["name"] == scope_name for s in r.json().get("scopes", []))

# Create scope if needed
if scope_exists(HOST, TOKEN, SCOPE):
    print(f" Scope '{SCOPE}' already exists - skiping creation.")
else:
    r = requests.post(
        f"{HOST}/api/2.0/secrets/scopes/create",
        headers={"Authorization": f"Bearer {TOKEN}"},
        json={"scope": SCOPE, "initial_manage_principal": "users"},
        timeout=20,
    )
    r.raise_for_status()
    print(f"Secret scope '{SCOPE}' created.")

# Put secret (if needed)
load_dotenv("../../.env")
w = WorkspaceClient()

try:
    w.secrets.get_secret("openai_scope", "openai_api_key")
    print(f"Secret '{SECRET_KEY}' already exists in scope {SCOPE} - skiping creation.")
except:
    w.secrets.put_secret(SCOPE, SECRET_KEY, string_value=os.environ["OPEN_AI_KEY"])
    print(f"Secret '{SECRET_KEY}' created in scope '{SCOPE}'.")

# COMMAND ----------

w.secrets.list_secrets(scope=SCOPE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create external chat agent endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC Use databricks sdk to create external endpoint.

# COMMAND ----------

endpoint_name = env_vars["SRAG_ENDPOINT_NAME"]
model = env_vars["MODEL"]

# COMMAND ----------

client = deployments.get_deploy_client("databricks")

# COMMAND ----------

client.list_endpoints()

# COMMAND ----------

client = deployments.get_deploy_client("databricks")
config = {
        "name": endpoint_name,
        "config": {
            "served_entities": [
                {
                    "external_model": {
                        "name": model,
                        "provider": "openai",
                        "task": "llm/v1/chat",
                        "openai_config": {
                            "openai_api_key": "{{secrets/openai_scope/openai_api_key}}",
                        },
                    },
                }
            ],
            "route_optimized": True,
        },
    }
try:
  client.get_endpoint(endpoint_name)
  print(f"Endpoint {endpoint_name} already exists, waiting to be ready...")
except Exception as e:
  if "RESOURCE_DOES_NOT_EXIST" in str(e):
    client.create_endpoint(config=config)
    print(f"Endpoint {endpoint_name} created.")
  else:
    raise

while not client.get_endpoint(endpoint_name)["state"]["ready"]:
  time.sleep(5)
print("Endpoint ready to use.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Endpoint

# COMMAND ----------

openai_client = w.serving_endpoints.get_open_ai_client()

resp = openai_client.chat.completions.create(
    model=endpoint_name,
    messages=[{"role": "user", "content": "What day is it today?"}],
    max_tokens=20,
)
print(resp.choices[0].message.content)

# COMMAND ----------

resp
