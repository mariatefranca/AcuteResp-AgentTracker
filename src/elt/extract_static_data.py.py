# Databricks notebook source
!pip install uv
!uv add  pdfplumber --active --quiet
!uv sync --active --quiet
dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import pdfplumber
import requests
import toml
from pyspark.sql.functions import col, sum
from pyspark.sql import functions as F


# COMMAND ----------

env_vars = toml.load("../../conf/env_vars.toml")

# COMMAND ----------

# MAGIC %md
# MAGIC # Extract data dictionary

# COMMAND ----------

url = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG/pdfs/Dicionario_de_Dados_SRAG_Hospitalizado_19.09.2022.pdf"
pdf_path = "/tmp/dicionario_dados.pdf"

# Download the PDF.
response = requests.get(url)
with open(pdf_path, "wb") as f:
    f.write(response.content)

rows = []
with pdfplumber.open(pdf_path) as pdf:
    for page in pdf.pages:
        tables = page.extract_tables()
        for table in tables:
            rows.extend(table)

# Convert to Pandas DataFrame.
pdf_df = pd.DataFrame(rows[1:], columns=rows[0])

# Fix problematic rows: if 'DBF' is empty, append text to previous row.
fixed_rows = []
for i, row in pdf_df.iterrows():
    if pd.isna(row["DBF"]) or str(row["DBF"]).strip() == "":
        # append each cell to the previous row (except DBF)
        for col in pdf_df.columns:
            if col != "DBF":
                fixed_rows[-1][col] = str(fixed_rows[-1][col]) + " " + str(row[col])
    else:
        fixed_rows.append(row.to_dict())

# Convert back to DataFrame.
pdf_df_fixed = pd.DataFrame(fixed_rows).drop(columns=["Tipo"])
pdf_df_fixed = pdf_df_fixed[['DBF', 'Nome do campo', 'Categoria', 'Descrição', 'Características']]

display(pdf_df_fixed)

# COMMAND ----------

pdf_df_fixed.columns

# COMMAND ----------

df_dictionary = spark.createDataFrame(pdf_df)
df_dictionary = df_dictionary.withColumnRenamed("Nome do campo", "Campo")
df_dictionary.write.mode("overwrite").saveAsTable(F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.data_dictionary')

# COMMAND ----------

srag_columns = ['NU_NOTIFIC',
 'DT_NOTIFIC',
 'DT_SIN_PRI',
 'SG_UF_NOT',
 'ID_MUNICIP OU\nCO_MUN_NOT',
 'ID_REGIONA OU\nCO_REGIONA',
 'CS_SEXO',
 'NU_IDADE_N',
 'TP_IDADE',
 'CS_GESTANT',
 'CS_ESCOL_N',
 'PAC_COCBO ou\nPAC_DSCBO',
 'ID_PAIS OU\nCO_PAIS',
 'NOSOCOMIAL',
 'AVE_SUINO',
 'FEBRE',
 'TOSSE',
 'GARGANTA',
 'DISPNEIA',
 'DESC_RESP',
 'SATURACAO',
 'DIARREIA',
 'VOMITO',
 'DOR_ABD',
 'FADIGA',
 'PERD_OLFT',
 'PERD_PALA',
 'OUTRO_SIN',
 'OUTRO_DES',
 'FATOR_RISC',
 'VACINA_COV',
 'VACINA',
 'DT_UT_DOSE',
 'MAE_VAC',
 'ANTIVIRAL',
 'TP_ANTIVIR',
 'OUT_ANTIV',
 'DT_ANTIVIR',
 'TRAT_COV',
 'TIPO_TRAT',
 'OUT_TRAT',
 'DT_TRT_COV',
 
 'HOSPITAL',
 'DT_INTERNA',
 'SG_UF_INTE',
 'ID_RG_INTE OU\nCO_RG_INTE',
 'ID_MN_INTE OU\nCO_MU_INTE',
 'ID_UN_INTE OU\nCO_UN_INTE',
 'UTI',
 'DT_ENTUTI',
 'DT_SAIDUTI',
 'SUPORT_VEN',
 'RAIOX_RES',
 'RAIOX_OUT',
 'DT_RAIOX',
 'TOMO_RES',
 'TOMO_OUT',
 'DT_TOMO',
 'AMOSTRA',
 'DT_COLETA',
 'TP_AMOSTRA',
 'OUT_AMOST',
 'REQUI_GAL',
 'TP_TES_AN',
 'DT_RES_AN',
 'RES_AN',
 'LAB_AN',
 'CO_LAB_AN',
 'POS_AN_FLU',
 'TP_FLU_AN',
 'POS_AN_OUT',
 'AN_SARS2',
 'AN_VSR',
 'AN_PARA1',
 'AN_PARA2',
 'AN_PARA3',
 'AN_ADENO',
 'AN_OUTRO',
 'DS_AN_OUT',
 'PCR_RESUL',
 'DT_PCR',
 'POS_PCRFLU',
 'TP_FLU_PCR',
 'PCR_FLUASU',
 'FLUASU_OUT',
 'PCR_FLUBLI',
 'FLUBLI_OUT',
 'POS_PCROUT',
 'PCR_ SARS2',
 'PCR_VSR',
 'PCR_PARA1',
 'PCR_PARA2',
 'PCR_PARA3',
 'PCR_PARA4',
 'PCR_ADENO',
 'PCR_METAP',
 'PCR_BOCA',
 'PCR_RINO',
 'PCR_OUTRO',
 'DS_PCR_OUT',
 'LAB_PCR OU\nCO_LAB_PCR',
 'TP_AM_SOR',
 'SOR_OUT',
 'DT_CO_SOR',
 'TP_SOR',
 'OUT_SOR',
 'SOR_OUT',
 'RES_IGG',
 'RES_IGM',
 'RES_IGA',
 'DT_RES',
 'CLASSI_FIN',
 'CLASSI_OUT',
 'CRITERIO',
 'EVOLUCAO',
 'DT_EVOLUCA',
 'DT_ENCERRA',
 'NU_DO',
 'OBSERVA',
 'NOME_PROF',
 'REG_PROF',
 'DT_DIGITA']

# COMMAND ----------

# MAGIC %md
# MAGIC # Extract Static SRAG data

# COMMAND ----------

# Load 2024 data and get schema
influenza_2024_url = "s3a://ckan.saude.gov.br/SRAG/2024/INFLUD24-26-06-2025.csv"
df_influenza_2024 = spark.read.options(delimiter=";", header=True).csv(influenza_2024_url, inferSchema=True, dateFormat="dd/MM/yyyy")

schema = df_influenza_2024.schema

# Load 2022 and 2023 data and enforce schema.
influenza_2022_url = "s3a://ckan.saude.gov.br/SRAG/2022/INFLUD22-26-06-2025.csv"
df_influenza_2022 = spark.read.options(delimiter=";", header=True).schema(schema).csv(influenza_2022_url, dateFormat="dd/MM/yyyy")

influenza_2023_url = "s3a://ckan.saude.gov.br/SRAG/2023/INFLUD23-26-06-2025.csv"
df_influenza_2023 = spark.read.options(delimiter=";", header=True).schema(schema).csv(influenza_2023_url, dateFormat="dd/MM/yyyy")

# COMMAND ----------

# Check row and columns count.
df_list = [df_influenza_2022, df_influenza_2023, df_influenza_2024]
df_names = ["df_influenza_2022", "df_influenza_2023", "df_influenza_2024"]
for df, name in zip(df_list, df_names):
    print(name, "num_rows = ", df.count(), ", num_cols = ", len(df.columns))

# COMMAND ----------

# Check column's match before merging the data.
cols_df_influenza_2022 = set(df_influenza_2022.columns)
cols_df_influenza_2023 = set(df_influenza_2023.columns)
cols_df_influenza_2024 = set(df_influenza_2024.columns)

print("Only in df_influenza_2023:", cols_df_influenza_2024 - cols_df_influenza_2023)
print("Only in df_influenza_2022:", cols_df_influenza_2024 - cols_df_influenza_2022)
print("In all:", len(cols_df_influenza_2023 & cols_df_influenza_2023 & cols_df_influenza_2024))

# COMMAND ----------

# Concat df_influenza_2024 and df_influenza_2023.
srag_data = (df_influenza_2024
             .union(df_influenza_2023)
             .union(df_influenza_2022)
)

# COMMAND ----------

# Display results.
srag_data.limit(5).toPandas()

# COMMAND ----------

# Check duplicates.
duplicates = srag_data.groupBy(srag_data.columns).count().filter("count > 1")
duplicates.count()

# COMMAND ----------

print("num_rows = ", srag_data.count())
print("num_cols = ", len(srag_data.columns))

# COMMAND ----------

# Write table.
table_name = F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.srag_vigilance'

srag_data.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw table EDA

# COMMAND ----------

srag_db = spark.read.table(table_name)

# COMMAND ----------

print("num_rows = ", srag_db.count())
print("num_cols = ", len(srag_db.columns))

# COMMAND ----------

sorted_columns = sorted(srag_db.columns)
display(
  srag_db
  .select(*sorted_columns)
  .limit(5)
  .toPandas()
)

# COMMAND ----------

selected_columns = [
    "NU_NOTIFIC",
    "DT_NOTIFIC",
    "DT_SIN_PRI",
    "SG_UF_NOT",
    "ID_MUNICIP",
    # "CO_MUN_NOT",
    "EVOLUCAO",
    "DT_EVOLUCA",
    "CLASSI_FIN",
    "NU_IDADE_N",
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
    # "ID_UNIDADE",
    # "CO_UN_INTE",
    "SUPORT_VEN",
    "VACINA_COV",
    "DOSE_1_COV",
    "DOSE_2_COV",
    "DOSE_REF",
    "DOSE_2REF",
    "FAB_COV_1",
    "FAB_COV_2",
    # "FAB_COVRF",
    # "FAB_COVRF2",
    "FAB_RE_BI",
    "VACINA",
    "DT_UT_DOSE",
    "MAE_VAC",
    "DT_VAC_MAE"
]

# COMMAND ----------

srag_filtered = srag_db.select(selected_columns)
srag_filtered.write.mode("overwrite").saveAsTable(F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.srag_filtered')


# COMMAND ----------

# # Null counts.
# null_data = [(c, srag_filtered.filter(col(c).isNull()).count()) for c in srag_filtered.columns]
# null_df = pd.DataFrame(null_data, columns=["column", "null_count"])
# null_df.sort_values(by="null_count", ascending=False)

# COMMAND ----------

srag_sample = srag_filtered.sample(withReplacement=None, fraction=0.1, seed=42)
srag_sample.write.mode("overwrite").saveAsTable(F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.srag_filtered_sample')

# COMMAND ----------

display(
  srag_db
  .select(selected_columns)
  .limit(5)
  .toPandas()
)

# COMMAND ----------

print("num_rows = ", srag_sample.count())
print("num_cols = ", len(srag_sample.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC # Extract Hospital Beds

# COMMAND ----------

hospital_filtered_columns = ["COMP", 
                             "CNES",
                             "UF", 
                             "REGIAO",
                             "MUNICIPIO",
                             "CO_CEP",
                             "LEITOS_EXISTENTES",
                             "LEITOS_SUS",
                             "UTI_TOTAL_EXIST",
                             "UTI_TOTAL_SUS",
                             "UTI_ADULTO_EXIST",
                             "UTI_ADULTO_SUS",
                             "UTI_PEDIATRICO_EXIST",
                             "UTI_PEDIATRICO_SUS",
                             "UTI_NEONATAL_EXIST",
                             "UTI_NEONATAL_SUS",
                             "UTI_QUEIMADO_EXIST",
                             "UTI_QUEIMADO_SUS",
                             "UTI_CORONARIANA_EXIST",
                             "UTI_CORONARIANA_SUS"
                             ]

# COMMAND ----------

# Load 2024 data and get schema
hospital_uti_2024_url = "s3a://ckan.saude.gov.br/Leitos_SUS/Leitos_2024.csv"
df_hospital_uti_2024 = spark.read.options(delimiter=",", header=True).csv(hospital_uti_2024_url, inferSchema=True, dateFormat="dd/MM/yyyy")

schema = df_hospital_uti_2024.schema

# Load 2023 and 2022 data and enforce schema.
hospital_uti_2023_url = "s3a://ckan.saude.gov.br/Leitos_SUS/Leitos_2023.csv"
df_hospital_uti_2023 = spark.read.options(delimiter=",", header=True).schema(schema).csv(hospital_uti_2023_url, dateFormat="dd/MM/yyyy").select(hospital_filtered_columns)

hospital_uti_2022_url = "s3a://ckan.saude.gov.br/Leitos_SUS/Leitos_2022.csv"
df_hospital_uti_2022 = spark.read.options(delimiter=",", header=True).schema(schema).csv(hospital_uti_2022_url, dateFormat="dd/MM/yyyy").select(hospital_filtered_columns)

df_hospital_uti_2024 = df_hospital_uti_2024.select(hospital_filtered_columns)

# COMMAND ----------

# Check row and columns count.
df_list = [df_hospital_uti_2022, df_hospital_uti_2023, df_hospital_uti_2024]
df_names = ["df_hospital_uti_2022", "df_hospital_uti_2023", "df_hospital_uti_2024"]
for df, name in zip(df_list, df_names):
    print(name, ": num_rows = ", df.count(), ", num_cols = ", len(df.columns))

# COMMAND ----------

# Check column's match before merging the data.
cols_df_hospital_uti_2022 = set(df_hospital_uti_2022.columns)
cols_df_hospital_uti_2023 = set(df_hospital_uti_2023.columns)
cols_df_hospital_uti_2024 = set(df_hospital_uti_2024.columns)

print("Only in df_hospital_uti_2023:", cols_df_hospital_uti_2024 - cols_df_hospital_uti_2023)
print("Only in df_hospital_uti_2022:", cols_df_hospital_uti_2024 - cols_df_hospital_uti_2022)
print("In all:", len(cols_df_hospital_uti_2023 & cols_df_hospital_uti_2023 & cols_df_hospital_uti_2024))

# COMMAND ----------

# Concat df_hospital_uti_2024, df_hospital_uti_2023 and df_hospital_uti_2022.
hospital_data = (df_hospital_uti_2024
                 .union(df_hospital_uti_2023)
                 .union(df_hospital_uti_2022)
)

# COMMAND ----------

# Check duplicates.
duplicates = hospital_data.groupBy(hospital_data.columns).count().filter("count > 1")
duplicates.count()

# COMMAND ----------

print("hospital_data: num_rows = ", hospital_data.count(), ", num_cols = ", len(hospital_data.columns))

# COMMAND ----------

# Write table.
hospital_table_name = F'{env_vars["CATALOG"]}.{env_vars["SCHEMA"]}.hospital'
hospital_data.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(hospital_table_name)
