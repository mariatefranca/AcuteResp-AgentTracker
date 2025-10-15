# Databricks notebook source
!pip install uv
!uv sync --active --quiet
dbutils.library.restartPython()

# COMMAND ----------

import pprint
import requests
import json
import pandas as pd
import toml
import requests, zipfile, io

# COMMAND ----------

env_vars = toml.load("../../conf/env_vars.toml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Request test

# COMMAND ----------

# Data SUS API link
data_sus_api_link = "https://opendatasus.saude.gov.br/api/3/action"

# Make the HTTP request
response = requests.get(f"{data_sus_api_link}/package_list")

# Use the json module to load CKAN's response into a dictionary
response_dict = json.loads(response.content)

# Check the contents of the response
assert response_dict['success'] is True  # make sure if response is OK

datasets = response_dict['result']         # extract all the packages from the response
print("Total datasets: ", len(datasets))                       # print the total number of datasets
datasets

# COMMAND ----------

def get_most_recent_data_url(package_id:str):

    # Base url for package information. 
    data_sus_api_link = 'https://opendatasus.saude.gov.br/api/3/action'

    # Construct the url for the package id.
    package_information_url = f"{data_sus_api_link}/package_show?id={package_id}"

    # Make the HTTP request
    package_information = requests.get(package_information_url)

    # Use the json module to load CKAN's response into a dictionary
    package_dict = json.loads(package_information.content)

    # Check the contents of the response.
    assert package_dict['success'] is True  # Make sure if response is OK

    for i in range(len(package_dict["result"]["resources"])):
        if (
            package_dict["result"]["resources"][i]["format"].lower() == "csv") & (
            "2025" in package_dict["result"]["resources"][i]["name"]
            ):
            url = package_dict["result"]["resources"][i]["url"]
            last_update = package_dict["result"]["resources"][i]["last_modified"]
        
    return url

# COMMAND ----------

# MAGIC %md
# MAGIC # 2025 SRAG - Banco Vivo

# COMMAND ----------

# SRAG package id.
srag_package_id = "39a4995f-4a6e-440f-8c8f-b00c81fae0d0"
latest_srag_update_url = get_most_recent_data_url(srag_package_id)
latest_srag_update_url

# COMMAND ----------

 # Correct the file path to use the s3a protocol when reading the file using spark.
corrected_srag_url = "s3a:/" + latest_srag_update_url.split("amazonaws.com")[1]
corrected_srag_url

# COMMAND ----------

srag_table_name = F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.srag_vigilance'
srag_table_2025 = F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.srag_2025'
srag_schema = spark.read.table(srag_table_name).schema

df_influenza_2025 = spark.read.options(delimiter=";", header=True).schema(srag_schema).csv(corrected_srag_url, dateFormat="dd/MM/yyyy")

# Save the new data to the SRAG table.
df_influenza_2025.write.mode("overwrite").saveAsTable(srag_table_2025)

# COMMAND ----------

df_influenza_2025.toPandas().tail(5)

# COMMAND ----------

print("df_influenza_2025: num_rows = ", df_influenza_2025.count(), ", num_cols = ", len(df_influenza_2025.columns))

# COMMAND ----------

srag_table = spark.read.table(srag_table_name)
row_count_before, col_count_before = srag_table.count(), len(srag_table.columns)
print(srag_table_name, ": num_rows = ", row_count_before, ", num_cols = ", col_count_before)

# Append 2025 new data to srag table.
spark.sql(f"""MERGE INTO {srag_table_name}
USING {srag_table_2025}
ON {srag_table_name}.NU_NOTIFIC = {srag_table_2025}.NU_NOTIFIC
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

srag_table_after = spark.read.table(srag_table_name)
print(srag_table_name, "after merging new data: num_rows = ", srag_table_after.count(), ", num_cols = ", len(srag_table_after.columns))
print("Number of new rows appended = ", srag_table_after.count() - srag_table.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # 2025 Hospital UTI

# COMMAND ----------

# HOSPITAL package id.
hospital_package_id = "791730b2-50bd-41ba-adf2-d915b88f712a"
latest_hospital_update_url = get_most_recent_data_url(hospital_package_id)
latest_hospital_update_url

# COMMAND ----------

hospital_table_name = F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.hospital'
hospital_table_name_2025 = F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.hospital_2025'
hospital_schema = spark.read.table(hospital_table_name).schema
hospital_columns = spark.read.table(hospital_table_name).columns

# COMMAND ----------

# Download zip file into memory.
r = requests.get(latest_hospital_update_url)
z = zipfile.ZipFile(io.BytesIO(r.content))

# Pick the first CSV name of zip file.
csv_filename = z.namelist()[0]

# Load into Pandas directly from the zip with the correct encoding
with z.open(csv_filename) as hospital_file:
    pd_df_hospital = pd.read_csv(
        hospital_file, 
        delimiter=";", 
        encoding='latin1', 
        on_bad_lines='skip'
    )[hospital_columns]

# Save the new hospital data into a. spark dataframe.
df_hospital_2025 = spark.createDataFrame(pd_df_hospital, schema=hospital_schema)

# COMMAND ----------

pd_df_hospital.head(2)

# COMMAND ----------

# Check column's match before merging the data.

cols_df_hospital_uti_all = set(hospital_columns)
cols_df_hospital_uti_2025 = set(df_hospital_2025.columns)

print("Only in df_hospital_uti_all:", cols_df_hospital_uti_2025 - cols_df_hospital_uti_all)
print("Only in df_hospital_uti_all:", cols_df_hospital_uti_all - cols_df_hospital_uti_2025)
print("In all:", len(cols_df_hospital_uti_all & cols_df_hospital_uti_2025))

# COMMAND ----------

# Save the new data into a table.
df_hospital_2025.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(hospital_table_name_2025)

# COMMAND ----------

hospital_table_2025 = spark.read.table(hospital_table_name_2025)
print("num_rows = ", hospital_table_2025.count())
print("num_cols = ", len(hospital_table_2025.columns))

# COMMAND ----------

hospital_table = spark.read.table(hospital_table_name)
row_count_before, col_count_before = hospital_table.count(), len(hospital_table.columns)
print(hospital_table_name, ": num_rows = ", row_count_before, ", num_cols = ", col_count_before)

# Append 2025 new data to hospital table.
spark.sql(f"""
MERGE INTO {hospital_table_name} AS target
USING {hospital_table_name_2025} AS source
ON target.CNES = source.CNES
AND target.COMP = source.COMP
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

hospital_table_after = spark.read.table(hospital_table_name)
print(hospital_table_name, "after merging new data: num_rows = ", hospital_table_after.count(), ", num_cols = ", len(hospital_table_after.columns))
print("Number of new rows appended = ", hospital_table_after.count() - row_count_before)
