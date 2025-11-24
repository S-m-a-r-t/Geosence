import streamlit as st
import clickhouse_connect
import numpy as np
import torch
from sentence_transformers import SentenceTransformer
from google import genai

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
CH_HOST = "b45p3y549w.ap-south-1.aws.clickhouse.cloud"
CH_USERNAME = "default"
CH_PASSWORD = "VPJUt8.YQVLeL"
GEMINI_API_KEY = "AIzaSyBvdhfJztMo5MMhAwxR8gLb0F6wJvnvCYA"

EMBED_TABLE = "gdelt.actor_pair_embeddings"
MAIN_TABLE = "gdelt.actor_pair"
MODEL_NAME = "gemini-2.5-flash"

# ---------------------------------------------------------
# INITIALIZE CLIENTS
# ---------------------------------------------------------
# ClickHouse client
ch = clickhouse_connect.get_client(
    host=CH_HOST,
    username=CH_USERNAME,
    password=CH_PASSWORD,
    secure=True,
)

# Gemini client
gai = genai.Client(api_key=GEMINI_API_KEY)

# Embedding model
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", device=DEVICE)
model.max_seq_length = 256

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
def emb_to_literal(vec):
    return "[" + ",".join(str(float(x)) for x in vec.tolist()) + "]"

# Vector search + join
def search_actor_pairs(query, top_k=10):
    q_emb = model.encode([query], convert_to_numpy=True)[0].astype(np.float32)
    literal = emb_to_literal(q_emb)

    sql = f"""
    SELECT 
        emb.actor1,
        emb.actor2,
        main.event_summary_text,
        cosineDistance(emb.embedding, {literal}) AS dist
    FROM {EMBED_TABLE} AS emb
    LEFT JOIN {MAIN_TABLE} AS main
      ON emb.actor1 = main.actor1 AND emb.actor2 = main.actor2
    ORDER BY dist ASC
    LIMIT {top_k}
    """

    return ch.query(sql).result_rows


def geopolity_analysis(query, rows):
    context = "\n\n".join(
        f"{a1}-{a2}: {txt}" 
        for (a1, a2, txt, dist) in rows
    )

    prompt = f"""
    You are a senior geopolitical analyst. Use the retrieved event summaries (which are cited below) to write a structured analysis.

    User Query:
    {query}

    Context (retrieved event summaries):
    {context}

    Produce:
    1) Short summary (3-5 sentences).
    2) Key drivers & actors (bulleted).
    3) Recent trends and signals (3 bullets).
    4) Short-term scenarios (1-3 bullets; 0-6 months).
    5) Long-term scenarios (1-3 bullets; 6-36 months).
    6) Uncertainties and missing info (what else you'd want).
    7) Suggested monitoring queries or data to watch (2-4 items).
    8) Provide citations inline using SOURCE[...] tags from the context. At the end, include a "Sources" block listing all source_ids and their actor pairs and distances.

    Be specific where possible and say "probabilities are approximate" when giving likelihoods. Use concise, direct language.
    """

    response = gai.models.generate_content(
        model=MODEL_NAME,
        contents=prompt
    )

    return response.text

# ---------------------------------------------------------
# STREAMLIT UI
# ---------------------------------------------------------
st.set_page_config(page_title="GeoSense", layout="wide")

st.title("🌍 GeoSense — Geopolitical RAG Engine")
st.write("Ask a geopolitical question and get an AI-powered analysis based on real event")

query = st.text_input("Enter your geopolitical query:", placeholder="e.g., future of India-China relations")

top_k = st.slider("Number of relevant events to fetch:", 3, 20, 8)

if st.button("Analyze"):
    if not query.strip():
        st.error("Please enter a query.")
    else:
        with st.spinner("Running semantic search…"):
            rows = search_actor_pairs(query, top_k)

        # Show retrieved items
        st.subheader("🔎 Retrieved Relevant Actor Interactions")
        for (a1, a2, txt, dist) in rows:
            st.markdown(f"""
            **{a1} – {a2}**  
            _distance: {dist:.3f}_  
            • {txt}  
            """)

        with st.spinner("Generating geopolitical analysis…"):
            answer = geopolity_analysis(query, rows)

        st.subheader("🧠 Geopolitical Analysis")
        st.write(answer)

st.markdown("---")
st.caption("Powered by ClickHouse + Sentence Transformers + Google Gemini")