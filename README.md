🌍 GeoSence
AI-Driven Geopolitical Intelligence & Analysis Platform

GeoSence is a high-performance geopolitics analytics system that processes massive GDELT datasets, performs fast OLAP queries using ClickHouse, generates vector embeddings for semantic search, and uses a Retrieval-Augmented Generation (RAG) pipeline with LLMs to deliver insightful geopolitical analysis through an interactive Streamlit dashboard.

🚀 Key Features
GDELT Data Pipeline
Automatic CSV merging, cleaning, and conversion to Parquet
High-speed processing using DuckDB, Polars & multiprocessing
Structured and optimized data ready for analytics
High-Performance Database
ClickHouse integration for fast OLAP queries
Efficient indexing, filtering & large dataset querying
Semantic Search + Embeddings
Generates embeddings for event text fields
Supports vector search using ClickHouse or FAISS
Enables natural language query retrieval
RAG (Retrieval-Augmented Generation)
User Query → Vector Search → Relevant Events → LLM → Final Analysis
Uses OpenAI API or compatible LLMs
Provides geopolitical summaries, insights, conflict timelines, etc.
Streamlit Frontend
Interactive dashboards
Query interface with LLM responses

🧠 Tech Stack
Backend & Data Engineering
Python
DuckDB
Polars / Pandas
Multiprocessing
ClickHouse
Parquet (optimized storage)
AI & Machine Learning
OpenAI LLM API
Sentence-Transformers / OpenAI Embeddings
Vector databases (ClickHouse/FAISS)
Frontend
Streamlit
Plotly / Matplotlib

📂 Project Structure
GeoSence/
│── data/
│   ├── raw/                   # Raw GDELT CSVs
│   ├── cleaned/               # Cleaned parquet files
│
│── scripts/
│   ├── merge_csvs.py          # Merging large GDELT files
│   ├── clean_data.py          # Cleaning & preprocessing
│   ├── load_clickhouse.py     # Loading data into ClickHouse
│   ├── create_embeddings.py   # Generating embeddings
│   ├── rag_pipeline.py        # Retrieval + LLM generation
│
│── frontend/
│   ├── app.py                 # Streamlit UI
│
│── models/                    # Saved models (if any)
│── utils/                     # Helper functions
│── README.md
│── requirements.txt

🛠️ Installation
1. Clone the Repo
git clone https://github.com/your-username/GeoSence.git
cd GeoSence

2. Create a Virtual Environment
python -m venv venv
source venv/bin/activate   # Linux / Mac
venv\Scripts\activate      # Windows

3. Install Dependencies
pip install -r requirements.txt

4. Install & Start ClickHouse
Download: https://clickhouse.com/

Start server:
clickhouse-server start

📊 Usage
1. Merge & Clean GDELT Data
python scripts/merge_csvs.py
python scripts/clean_data.py

2. Load into ClickHouse
python scripts/load_clickhouse.py

3. Generate Embeddings
python scripts/create_embeddings.py

4. Start Streamlit App
streamlit run frontend/app.py

🧩 How the System Works (Architecture)
        ┌──────────────────────┐
        │  GDELT Raw Dataset   │
        └──────────┬───────────┘
                   │
                   ▼
     ┌────────────────────────────┐
     │   Data Cleaning (DuckDB)   │
     └──────────┬─────────────────┘
                │ Parquet Output
                ▼
     ┌────────────────────────────┐
     │  ClickHouse (OLAP Engine)  │
     └──────────┬─────────────────┘
                │
                ▼
     ┌────────────────────────────┐
     │   Vector Embeddings (AI)   │
     └──────────┬─────────────────┘
                │
                ▼
     ┌────────────────────────────┐
     │   RAG Pipeline + LLM       │
     └──────────┬─────────────────┘
                │
                ▼
     ┌────────────────────────────┐
     │   Streamlit Web App        │
     └────────────────────────────┘
🔮 Future Enhancements
Add real-time news ingestion
Integrate temporal forecasting (LSTM/Prophet)
Multi-language geopolitics query support
GPU-accelerated embeddings
Automated trend detection
Deploy as Dockerized microservices

📜 License
MIT License (Feel free to modify or extend)

🤝 Contributing
Pull requests and feature suggestions are welcome!
Please open an issue before major changes.
