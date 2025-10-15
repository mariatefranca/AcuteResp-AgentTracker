# Databricks notebook source
!pip install uv
!uv sync --active --quiet
dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import toml
import pyspark.sql.functions as F
from pyspark.sql import Window
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql.connect.dataframe import DataFrame

# COMMAND ----------

env_vars = toml.load("../../conf/env_vars.toml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Store

# COMMAND ----------

# MAGIC %md
# MAGIC Cria uma Feature Store para armazenar as variáveis de dados estruturados a serem usadas pelo modelo.

# COMMAND ----------

fe = FeatureEngineeringClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Srag Feature Store

# COMMAND ----------

srag_source_table_name = F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.srag_vigilance'
srag_feature_store_table = f'{env_vars["CATALOG"]}.{env_vars["FS_SCHEMA"]}.srag_features'

# COMMAND ----------

srag_filtered = spark.read.table(srag_source_table_name)

# COMMAND ----------

isinstance(
    srag_filtered,
    DataFrame
)

# COMMAND ----------

columns_to_filter = [
    "NU_NOTIFIC",
    "DT_NOTIFIC",
    "DT_SIN_PRI",
    "SG_UF_NOT",
    "ID_MUNICIP",
    "EVOLUCAO",
    "DT_EVOLUCA",
    "CLASSI_FIN",
    "NU_IDADE_N",
    'TP_IDADE',
    "CS_SEXO",
    "FATOR_RISC",
    "CARDIOPATI",
    "DIABETES",
    "IMUNODEPRE",
    "OBESIDADE",
    "HOSPITAL",
    "DT_INTERNA",
    "UTI",
    "DT_ENTUTI",
    "DT_SAIDUTI",
    "SUPORT_VEN",
    "VACINA_COV",
    "DOSE_1_COV",
    "DOSE_2_COV",
    "DOSE_REF",
    "DOSE_2REF",
    "VACINA",
    "DT_UT_DOSE",
    "MAE_VAC",
    "DT_VAC_MAE"
]

# COMMAND ----------

srag_filtered = srag_filtered.select(columns_to_filter)

# COMMAND ----------

# Create new features.
srag_filtered = srag_filtered.withColumns({
    "obito_srag": F.when(F.col("EVOLUCAO") == 2, 1).otherwise(0),
    "alta": F.when(F.col("EVOLUCAO") == 1, 1).otherwise(0),
    "dias_internacao_uti": F.when(F.col("DT_SAIDUTI").isNotNull(), F.datediff(F.col("DT_SAIDUTI"), F.col("DT_ENTUTI"))).otherwise(F.datediff(F.col("DT_EVOLUCA"), F.col("DT_ENTUTI"))),
    "idade_anos": F.when(F.col("TP_IDADE") == 1, F.round(F.col("NU_IDADE_N")/365, 2)).when(F.col("TP_IDADE") == 2, F.round(F.col("NU_IDADE_N")/12, 2)).otherwise(F.col("NU_IDADE_N")),
    "vacinacao_covid": F.when(F.col("VACINA_COV") == 1, 1).otherwise(0),
    "vacinacao_influenza": F.when(F.col("VACINA") == 1, 1).otherwise(0),
    })
    
srag_filtered = srag_filtered.withColumns({
        "classificacao_etaria_leito": F.when(F.col("idade_anos") <= 0.0768, F.lit("neonatal")).when(F.col("idade_anos") >= 12, F.lit("adulto")).otherwise(F.lit("pediatrica")),
    })

# COMMAND ----------

srag_filtered.limit(5).toPandas()

# COMMAND ----------

print("num_rows = ", srag_filtered.count())
print("num_cols = ", len(srag_filtered.columns))

# COMMAND ----------

# Contagem de Valores Distintos por Coluna
distinct_counts = [
    (col_name, srag_filtered.select(col_name).distinct().count())
    for col_name in srag_filtered.columns
]
pd.DataFrame(
    distinct_counts,
    columns=["column", "distinct_count"]
)

# COMMAND ----------

display(srag_filtered)

# COMMAND ----------

dups = (
    srag_filtered
    .groupBy("NU_NOTIFIC")
    .count()
    .filter("count > 1")
)
display(dups)

# COMMAND ----------

# Create feature table with selected features using "NU_NOTIFIC" as the primary key.
srag_feature_table = fe.create_table(
  name=srag_feature_store_table,
  primary_keys='NU_NOTIFIC',
  schema=srag_filtered.schema,
  description='SRAG features'
)

# COMMAND ----------

fe.write_table(
  name=f"{env_vars['CATALOG']}.{env_vars['FS_SCHEMA']}.srag_features",
  df=srag_filtered,
  mode="merge",
)

# COMMAND ----------

fs = spark.read.table(f"{env_vars['CATALOG']}.{env_vars['FS_SCHEMA']}.srag_features")

# COMMAND ----------

# MAGIC %md
# MAGIC # SRAG features dictionary

# COMMAND ----------

dictionary = spark.read.table(f"{env_vars['CATALOG']}.{env_vars['SCHEMA']}.data_dictionary")

# COMMAND ----------

# Get the list of columns in fs
fs_columns = fs.columns

# Filter the dictionary DataFrame and rename columns
fs_dictionary = (
    dictionary.filter(F.col("DBF").isin(fs_columns))
    .select(F.col("DBF"), F.col("Descrição"))
    .withColumnRenamed("Descrição", "descricao")
    .withColumnRenamed("DBF", "coluna")
)
# Replace newline characters with spaces.
fs_dictionary = fs_dictionary.withColumn(
    "descricao",
    F.regexp_replace(F.col("descricao"), r"\n\s*", " ")
)

# Collect the 'coluna' values as a Python list
dbf_values = [row["coluna"] for row in fs_dictionary.select("coluna").distinct().collect()]

# Compute the difference
new_columns = [col for col in fs_columns if col not in dbf_values]

# COMMAND ----------

# Dictionary with new columsn descriptions.
colunas_descricoes = {
    "ID_MUNICIP": "Município da Notificação", 
    "obito_srag": "Indica se o paciente evoluiu para óbito por SRAG (1 = sim, 0 = não)",
    "alta": "Indica se o paciente recebeu alta hospitalar (1 = sim, 0 = não)",
    "dias_internacao_uti": "Número de dias de internação na UTI. Se DT_SAIDUTI não estiver preenchida, calcula até DT_EVOLUCA",
    "idade_anos": "Idade do paciente em anos (conversão de dias ou meses para anos)",
    "vacinacao_covid": "Indica se o paciente recebeu vacinação contra COVID-19 (1 = sim, 0 = não)",
    "vacinacao_influenza": "Indica se o paciente recebeu vacinação contra influenza (1 = sim, 0 = não)",
    "classificacao_etaria_leito": "Classificação etária do paciente para alocação de leito (neonatal, pediatrica ou adulto)"
}

# Transformando o dicionário em lista de tuplas
lista_colunas = [(k, v) for k, v in colunas_descricoes.items()]

# Create a DataFrame Spark with columns 'coluna' and 'descricao'
df_descricoes = spark.createDataFrame(lista_colunas, schema=["coluna", "descricao"])



# COMMAND ----------


df_descricoes.toPandas()

# COMMAND ----------

# Merge with filtered dictionary
fs_dictionary = fs_dictionary.unionByName(df_descricoes)


# COMMAND ----------

fs_dictionary_table = f"{env_vars['CATALOG']}.{env_vars['FS_SCHEMA']}.srag_features_dictionary"
# Write the updated dictionary to the feature store
# Create feature table with selected features using "coluna" as the primary key.
srag_feature_dictionary = fe.create_table(
  name=fs_dictionary_table,
  primary_keys='coluna',
  schema=fs_dictionary.schema,
  description='SRAG features descriptions'
)

fe.write_table(
  name=fs_dictionary_table ,
  df=fs_dictionary,
  mode="merge",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hospital Feature Store

# COMMAND ----------

hospital_data = spark.read.table(F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.hospital')
hospital_feature_store_table = f'{env_vars["CATALOG"]}.{env_vars["FS_SCHEMA"]}.hospital_features'

# COMMAND ----------

hospital_features = hospital_data.filter(F.col("COMP") > 202500).groupBy("UF", "COMP").agg(
  F.sum("UTI_ADULTO_EXIST").alias("adulto"),
  F.sum("UTI_PEDIATRICO_EXIST").alias("pediatrica"),
  F.sum("UTI_NEONATAL_EXIST").alias("neonatal"),
).withColumns({"uf_month_year": F.concat(F.col("UF"), F.col("COMP")),
               "month_year": F.to_date(F.col("COMP").cast("string"), "yyyyMM")
}).drop("COMP")


# COMMAND ----------

hospital_features.schema

# COMMAND ----------

# Create a feature table of hospital data with selected features using "uf_month_year" as the primary key.
hospital_feature_table = fe.create_table(
  name=hospital_feature_store_table,
  primary_keys='uf_month_year',
  schema=hospital_features.schema,
  description='Hospital uci beds'
)

# COMMAND ----------

fe.write_table(
  name=hospital_feature_store_table,
  df=hospital_features,
  mode="merge",
)
