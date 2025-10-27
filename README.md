Activación del venv: source venv/bin/activate


Estructura del proyecto:

RAG_System/
│
├── data/
│   ├── reglamentos_pdfs/
│   └── rag_parquet/
│       └── chunks/
│
├── src/
│   ├── ingest_spark.py
│   ├── generate_embeddings_spark.py
│   └── rag_service.py      <-- aquí pondremos el FastAPI luego
│
├── venv/
└── requirements.txt
