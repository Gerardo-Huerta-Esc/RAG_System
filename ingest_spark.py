# ingest_spark.py
from pyspark.sql import SparkSession, functions as F, types as T
from pdfminer.high_level import extract_text
import os, re

# Inicializa Spark
spark = (
    SparkSession.builder
    .appName("RAG_Ingest")
    .getOrCreate()
)

# Ruta de entrada
PDF_PATH = "./data/reglamentos_pdfs/"
OUTPUT_PATH = "./data/rag_parquet/chunks/"

# Función auxiliar para extraer texto de un PDF
def read_pdf_text(path):
    try:
        text = extract_text(path)
        text = re.sub(r"\s+", " ", text)  # limpia saltos de línea
        return text.strip()
    except Exception as e:
        return f"[ERROR] {e}"

# Registrar como UDF Spark
read_pdf_udf = F.udf(read_pdf_text, T.StringType())

# Crear DataFrame con lista de archivos
files = [os.path.join(PDF_PATH, f) for f in os.listdir(PDF_PATH) if f.endswith(".pdf")]
df_files = spark.createDataFrame([(f,) for f in files], ["path"])

# Extrae texto
df_texts = df_files.withColumn("text", read_pdf_udf(F.col("path")))

# Divide el texto en chunks de ~800 palabras
def chunk_text(text, chunk_size=800, overlap=200):
    if not text or text.startswith("[ERROR]"):
        return []
    words = text.split()
    chunks = []
    i = 0
    while i < len(words):
        chunk = " ".join(words[i:i + chunk_size])
        chunks.append(chunk)
        i += chunk_size - overlap
    return chunks

chunk_udf = F.udf(chunk_text, T.ArrayType(T.StringType()))

df_chunks = (
    df_texts
    .withColumn("chunks", chunk_udf(F.col("text")))
    .selectExpr("path", "posexplode(chunks) as (chunk_id, text_chunk)")
)

# Guarda en Parquet
df_chunks.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"Ingestión completada. Parquet guardado en {OUTPUT_PATH}")
spark.stop()
