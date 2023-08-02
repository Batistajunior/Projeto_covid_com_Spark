#!/usr/bin/env python
# coding: utf-8

# In[10]:


# Importe as bibliotecas necessárias
from pyspark.sql import SparkSession

# Cria uma sessão Spark
spark = SparkSession.builder.appName("CasosAcumulados").getOrCreate()

# Carrega os dados do arquivo CSV com delimitador de ponto e vírgula (;) e com cabeçalho
file_path = "/Users/batistajunior/Downloads/Projeto_covid_dados/covid_dados.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True, sep=";")

# Mostra o DataFrame completo
df.show(truncate=False)

# Seleciona a coluna de interesse para calcular a soma dos casos acumulados
cases_sum = df.groupBy("nome").sum("casosAcumulado").withColumnRenamed("sum(casosAcumulado)", "casosAcumuladosTotais")

# Exibe os resultados
cases_sum.show()

# Encerra a sessão Spark
spark.stop()


# In[11]:


# Importe as bibliotecas necessárias
from pyspark.sql import SparkSession

# Cria uma sessão Spark
spark = SparkSession.builder.appName("CasosAcumulados").getOrCreate()

# Carrega o arquivo CSV com delimitador de vírgula (,) e com cabeçalho
file_path = "/Users/batistajunior/Downloads/Projeto_covid_dados/covid_dados.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True, sep=";")

# Mostra o esquema do DataFrame (opcional)
df.printSchema()

# Seleciona a coluna de interesse para calcular a soma dos casos acumulados
cases_sum = df.groupBy("nome").sum("casosAcumulado").withColumnRenamed("sum(casosAcumulado)", "casosAcumuladosTotais")

# Exibe os resultados
cases_sum.show()

# Encerra a sessão Spark
spark.stop()


# In[ ]:




