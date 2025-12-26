# ==============================================================================
# I. IMPORTS & INITIAL SETUP
# ==============================================================================
import os
import json
import io
import uuid
from datetime import date, datetime
from typing import List, Dict, Any, Optional
import asyncio

# Core application libraries
import uvicorn
import shutil
import tempfile
import subprocess
from pathlib import Path
from fastapi import FastAPI, HTTPException, Form, File, UploadFile, Request, Response, Cookie, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocketState
from dotenv import load_dotenv
from pydantic import BaseModel

# Database and data handling
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
import pandas as pd
import simplejson

# AI and Machine Learning
from google.api_core import exceptions as google_exceptions
import google.generativeai as genai

# Visualization
import plotly.express as px

# Caching and session management
import redis

# encryption: prefer the backend package, fall back to relative import or a
# globally installed package. This resolves the common issue where a
# top-level package named `security` (from PyPI) shadows our local module.
import importlib

_security = None
for mod_name in ("backend.security", "backend.security", "security"):
    try:
        _security = importlib.import_module(mod_name)
        break
    except Exception:
        continue

# Also try a relative import if running as a package module
if _security is None:
    try:
        from . import security as _security
    except Exception:
        _security = None

encrypt_text = getattr(_security, 'encrypt_text', None)
decrypt_text = getattr(_security, 'decrypt_text', None)

# frontend response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

# plotly
import plotly.io as pio
import plotly.graph_objs as go

# speech: prefer backend.audio, fall back to relative or top-level audio
_audio_mod = None
for mod_name in ("backend.audio", "audio"):
    try:
        _audio_mod = importlib.import_module(mod_name)
        break
    except Exception:
        _audio_mod = None

if _audio_mod is None:
    try:
        from . import audio as _audio_mod
    except Exception:
        _audio_mod = None

if _audio_mod is not None:
    audio_router = getattr(_audio_mod, 'router', None)
    text_to_speech_async = getattr(_audio_mod, 'text_to_speech_async', None)
else:
    audio_router = None
    text_to_speech_async = None

# Load environment variables from a .env file
load_dotenv()


# ==============================================================================
# II. ENVIRONMENT & CONFIGURATION
# ==============================================================================

# --- AI/ML API Configuration ---
try:
    gemini_api_key = os.getenv("GOOGLE_API_KEY")
    if not gemini_api_key:
        print("WARNING: GOOGLE_API_KEY environment variable not set. Gemini features will fail.")
    else:
        genai.configure(api_key=gemini_api_key)
except Exception as e:
    print(f"ERROR: Failed to configure Gemini API: {e}")

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Intelligent Data Assistant API",
    description="An API that translates natural language to SQL, generates visualizations, and provides data insights.",
    version="1.0.0" # Version updated
)

app.include_router(audio_router)

# Mount static files directory. Resolve path relative to this file so importing
# the module doesn't fail when the current working directory differs.
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
else:
    print(f"WARNING: Static directory '{STATIC_DIR}' does not exist. Skipping static mount.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

active_tasks: dict[str, asyncio.Task] = {}

# --- Pydantic Request Model ---
class AskRequest(BaseModel):
    question: str
    pipeline: Optional[str] = None


# --- Session Management Middleware ---
@app.middleware("http")
async def session_middleware(request: Request, call_next):
    user_id = request.cookies.get("user_id")
    session_id = request.cookies.get("session_id")

 
    new_user_id = user_id or str(uuid.uuid4())
    new_session_id = session_id or str(uuid.uuid4())
    request.state.user_id = new_user_id
    request.state.session_id = new_session_id

    # Proceed to the actual handler
    response = await call_next(request)

    # Set cookies on the response if they were newly generated
    if not user_id:
        response.set_cookie(key="user_id", value=new_user_id, httponly=True, max_age=60*60*24*365, samesite="Lax")
    if not session_id:
        response.set_cookie(key="session_id", value=new_session_id, httponly=True, samesite="Lax")

    return response


# --- Redis Configuration for Chat History ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def redis_available() -> bool:
    """Checks if the Redis server is available."""
    try:
        redis_client.ping()
        return True
    except redis.exceptions.ConnectionError:
        print("WARNING: Redis connection failed. Chat history and caching will be unavailable.")
        return False


# --- Global Cache for Database Schema ---
schema_cache = {}
db_pool = None


# ==============================================================================
# III. DATABASE & SCHEMA FUNCTIONS
# ==============================================================================

def extract_mysql_schema_json(pool: MySQLConnectionPool) -> Dict[str, Any]:
    """Extracts the database schema using a connection from the pool."""
    conn = None
    try:
        conn = pool.get_connection()
        cursor = conn.cursor()
        database_name = conn.database
        cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in cursor.fetchall()]

        schema = {}
        for table in tables:
            cursor.execute(f"DESCRIBE `{table}`;")
            columns = cursor.fetchall()
            schema[table] = [
                {"Field": col[0], "Type": col[1], "Null": col[2], "Key": col[3], "Default": col[4], "Extra": col[5]}
                for col in columns
            ]
        print(f"Schema successfully extracted for database '{database_name}'.")
        return schema
    except mysql.connector.Error as err:
        print(f"Database schema extraction error: {err}")
        raise HTTPException(status_code=500, detail=f"Database error: {err}")
    finally:
        if conn and conn.is_connected():
            conn.close()


def execute_sql_query_mysql(pool: MySQLConnectionPool, sql: str) -> List[Dict]:
    """Executes a SQL query using a connection from the pool."""
    conn = None
    try:
        conn = pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        print(f"Executing SQL query: {sql}")
        cursor.execute(sql)
        results = cursor.fetchall()
        print(f"SQL query executed. Rows returned: {len(results)}")
        return results
    except mysql.connector.Error as e:
        print(f"SQL execution error: {e}")
        raise HTTPException(status_code=400, detail=f"SQL execution error: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()


# ==============================================================================
# IV. CHAT HISTORY (REDIS) FUNCTIONS
# ==============================================================================

def save_interaction_to_history(user_id: str, session_id: str, question: str, sql: str, pipeline: str):
    """Saves a user question, the generated SQL, and the pipeline type to the chat history."""
    if not redis_available():
        return
    key = f"chat:{user_id}:{session_id}"
    interaction = json.dumps({"question": question, "sql": sql, "pipeline": pipeline})
    try:
        redis_client.rpush(key, interaction)
        redis_client.expire(key, 86400)  # 24 hours
    except redis.exceptions.ConnectionError as e:
        print(f"ERROR: Could not save to Redis due to connection error: {e}")


def get_session_history(user_id: str, session_id: str, n: int = 5) -> List[str]:
    """Retrieves the last N interactions from the chat history."""
    if not redis_available():
        return []
    key = f"chat:{user_id}:{session_id}"
    total_items = redis_client.llen(key)
    # The history items are JSON strings, so they need to be parsed by the consumer
    history_json_strings = redis_client.lrange(key, max(0, total_items - n), total_items - 1)
    return history_json_strings

def parse_history_items(raw_history: list) -> list[dict]:
    """Safely parse Redis history JSON strings into a list of dicts."""
    parsed = []
    for item in raw_history:
        try:
            obj = json.loads(item)
            if isinstance(obj, str):
                obj = json.loads(obj) 
            if isinstance(obj, dict):
                parsed.append(obj)
        except Exception as e:
            print(f"Skipping invalid history item: {item} ({e})")
    return parsed


def clear_session_history(user_id: str, session_id: str):
    """Clears the chat history for a given user and session."""
    if not redis_available():
        return
    key = f"chat:{user_id}:{session_id}"
    redis_client.delete(key)
    print(f"Cleared history for user: {user_id}, session: {session_id}")


# ==============================================================================
# V. LLM PROMPT GENERATION FUNCTIONS
# ==============================================================================

def generate_intent_classification_prompt(question: str) -> str:
    """Generates a prompt to classify the user's intent."""
    return f'''
Classify the user's intent based on their question into one of three categories: "sql", "visualization", or "insights".
Your output must be a single word.

- "visualization": For questions that explicitly ask for a chart, plot, graph, or any visual representation.
  Examples: "Draw a bar chart of sales per region.", "Can you plot the user growth over time?"

- "insights": For questions that ask for a summary, trends, patterns, or a high-level overview.
  Examples: "Tell me about our top customers.", "What are the key trends in product performance?", "Summarize sales."

- "sql": For all other questions that ask to retrieve, show, list, count, or calculate specific data points.
  Examples: "Show me the latest orders.", "How many users signed up last week?", "What is the total revenue?"

Question: "{question}"

Intent:'''

def generate_sql_query_prompt(schema_info: dict, question: str, history: List[Dict]) -> str:
    """Generates a production-ready prompt for the Gemini model to create SQL queries."""
    formatted_schema_parts = []
    for table, columns in schema_info.items():
        cols = [f"{col['Field']} ({col['Type']})" for col in columns]
        formatted_schema_parts.append(
            f"- Table `{table}` has columns: {', '.join(cols)}"
        )
    formatted_schema = "\n".join(formatted_schema_parts)


    
    formatted_history = "\n".join(
        f"- Previous Question: {h['question']}\n  Generated SQL: {h['sql']}" for h in history
    ) if history else "No previous questions in this session."

    return f'''You are an expert MySQL data analyst. Your task is to convert a natural language question into a precise and executable MySQL query.
Your ONLY output must be a single JSON object with the key "sql_query".

### DATABASE SCHEMA:
{formatted_schema}

### CONVERSATION HISTORY:
{formatted_history}

### INSTRUCTIONS:
1.  **Analyze the "CURRENT QUESTION" in the context of the "CONVERSATION HISTORY" and the database schema.** Treat this as an ongoing conversation if and only if the current question refers back to previous questions or answers.
2.  Use the history to resolve ambiguities. For example, if the user asks "what about for product X?", look at the previous question to understand the original metric they were asking about.
3.  Generate a single, precise MySQL query that accurately answers the question.
4.  Use table aliases (T1, T2) for clarity in joins.
5.  Ensure all functions are MySQL compatible (e.g., use `NOW()` for the current timestamp).
7.  If unable to find the required table, see the closest available table and use it as a reference. e.g if question is about "albums", and the table "albums" is missing from the schema , find the closest table to "albums", here it is "album".
6.  Your ONLY output must be a single JSON object with the key "sql_query". Do NOT include any markdown, comments, or explanations.

## FEW SHOT EXAMPLE:
1. CURRENT QUESTION: "Show me the names of the three most recently hired employees from there."
 PREVIOUS QUESTIONS: "Which department is the largest?"
 CHAIN OF THOUGHT: 
       "1. Identify Targets & Conditions: I need to find user names from the users table who meet two criteria: signed up in 2024 and have no corresponding entries in the orders table.
        2. Plan the Logic: First, I'll filter users for a signup_date within 2024. Then, I'll find which of those users do not have their user_id present in the orders table, which is a perfect use case for a LEFT JOIN and checking for NULL or using a NOT IN subquery.
        3. Synthesize the Query: Combine the filtering and the anti-join logic to select the name from the users table where the year of signup_date is 2024 and their user_id is not found in the orders table."
SQL QUERY: "SELECT name FROM users WHERE YEAR(signup_date) = 2024 AND user_id NOT IN (SELECT user_id FROM orders);"

2. CURRENT QUESTION: "List the 5 longest tracks from that artist's albums."
   PREVIOUS QUESTIONS: "Who is the artist for the album 'Let There Be Rock'?"
   CHAIN OF THOUGHT:
        "1. Deconstruct & Correct: The user wants the 5 longest "tracks" from all "albums" by the artist 'AC/DC' (inferred from the history). The query uses plural table names ("albums", "tracks"), but the schema has singular names (album, track); I will use the closest available table names.
         2. Plan Logic: I need to find the artist_id for 'AC/DC', find all album_ids associated with that artist, find all tracks from those albums, and then sort them by milliseconds descending, taking the top 5. A series of JOINs from artist to album to track will work.
         3. Synthesize Query & Format: I'll JOIN the tables, WHERE the artist name is 'AC/DC', ORDER BY the track duration (milliseconds) DESC, LIMIT the result to 5, and then format the final SQL string into the required JSON object."
   SQL QUERY: "SELECT T1.name FROM track AS T1 INNER JOIN album AS T2 ON T1.album_id = T2.album_id INNER JOIN artist AS T3 ON T2.artist_id = T3.artist_id WHERE T3.name = 'AC/DC' ORDER BY T1.milliseconds DESC LIMIT 5"


### CURRENT QUESTION:
"{question}"

### JSON OUTPUT:
'''

def generate_sql_nlg_prompt(question: str, sql_query: str, results: List[Dict]) -> str:
    """Generates a prompt for a natural language summary of SQL query results."""
    serializable_results = simplejson.dumps(results[:20], default=str, use_decimal=True, ignore_nan=True)
    
    return f'''You are an AI data assistant. Your task is to provide a clear and direct natural language answer to a user's question based on the provided data.
The response should be suitable for being read aloud.

### User's Original Question:
"{question}"

### Executed SQL Query:
`{sql_query}`

### Data Results (JSON format):
```json
{serializable_results}
```

### INSTRUCTIONS:
- Directly answer the user's question using the data.
- If the results are empty, state: "THE DATABASE WAS EMPTY FOR THE FOLLOWING QUESTION."
- Keep the response concise and factual. Avoid conversational filler.

### Your Spoken Response:
'''

def generate_chart_metadata_prompt(question: str, sql_query: str, results: List[Dict]) -> str:
    """Generates a prompt to determine the best chart type and its metadata."""
    serializable_results = simplejson.dumps(results, default=str, use_decimal=True, ignore_nan=True)

    return f'''You are an AI data visualization assistant. Given a user's question and data, determine the most suitable chart and the columns needed to create it.
Your ONLY output must be a single JSON object.

### User's Original Question:
"{question}"

### SQL Query Executed:
`{sql_query}`

### Database Results:
```json
{serializable_results}
```

### JSON OUTPUT INSTRUCTIONS:
- If visualization is not suitable or the data is empty, set "chart_type": "none".
- Choose the best `chart_type` from: "bar", "pie", "line", "histogram", "violin", "box".
- For "bar" and "line" charts, provide `x_axis_column` and `y_axis_column`.
- For "pie" charts, provide `label_column` and `value_column`.
- For "histogram", "violin", and "box" plots (which show distributions), provide a single `value_column`.
- Provide a concise `title` for the chart that reflects the user's question.

### Your JSON Output:
'''

def generate_visualization_nlg_prompt(question: str, chart_metadata: Dict, results: List[Dict]) -> str:
    """Generates a prompt for a natural language summary of a data visualization."""
    serializable_results = simplejson.dumps(results, default=str, use_decimal=True, ignore_nan=True)

    return f'''You are a data visualization expert. Your task is to provide a short, insightful summary that describes the main finding in a chart.
This summary will be displayed alongside the chart.

### User's Question that Led to the Chart:
"{question}"

### Chart Details:
- Type: {chart_metadata.get("chart_type", "N/A")}
- Title: {chart_metadata.get("title", "N/A")}

### Data Used for the Chart (JSON format):
```json
{serializable_results}
```

### INSTRUCTIONS:
- Based on the data, provide a 1-2 sentence summary of the key insight.
- Focus on the primary trend, comparison, or distribution shown. What is the most important message the user should take away from this visual?
- If the data is empty, state that "No data was available to generate the chart."

### Your Chart Summary:
'''

def generate_insight_chunk_prompt(chunk_df: pd.DataFrame, question: str) -> str:
    """Generates a prompt to get insights from a small chunk of data."""
    return f"""
You are a data analyst reviewing a small sample of a larger dataset.
The data was retrieved to answer the user's question: "{question}"

### Data Sample (JSON format):
{chunk_df.to_json(orient="records", indent=2)}

### INSTRUCTIONS:
- Breifly describe the key patterns, trends, or interesting data points in THIS SPECIFIC SAMPLE.
- Do not make conclusions about the entire dataset, only this chunk.
- Output a few bullet points.

### Your Analysis of this Chunk:
"""

def generate_insight_summary_prompt(chunk_insights: List[str], question: str) -> str:
    """Generates a prompt to merge insights from multiple chunks into a final summary."""
    formatted_insights = "\n\n".join(f"--- Insight from Chunk {i+1} ---\n{insight}" for i, insight in enumerate(chunk_insights))
    
    return f"""
You are a senior data strategist. You have received analyses from junior analysts who each looked at a small piece of a large dataset.
Your task is to synthesize these individual observations into a single, coherent, high-level summary for an executive.

### Original User Question:
"{question}"

### Individual Chunk Analyses:
{formatted_insights}

### INSTRUCTIONS:
- Read all the chunk analyses to understand the overall picture.
- Identify recurring themes, trends, and significant findings that appear across multiple chunks.
- Write a single, cohesive summary that answers the user's original question.
- Do not just list the individual insights. Synthesize them into a narrative.
- Start with the most important finding.

### Your Final Executive Summary:
"""

def generate_typed_recommendations_prompt(schema_info: dict, question: str, sql_query: str, results: List[Dict], history: List[Dict]) -> str:
    """Generates a prompt to create intelligent follow-up question recommendations based on context."""
    formatted_schema_parts = []
    for table, columns in schema_info.items():
        cols = [f"{col['Field']} ({col['Type']})" for col in columns]
        formatted_schema_parts.append(
            f"- Table `{table}` has columns: {', '.join(cols)}"
        )
    formatted_schema = "\n".join(formatted_schema_parts)
    results_sample = results[:5] if results else []
    formatted_results = json.dumps(results_sample, default=str, indent=2)
    formatted_history = "\n".join(
        f"- Previous Q: {h['question']}\n  SQL: {h['sql']}" for h in history[-3:]
    ) if history else "No previous questions in this session."

    return f"""You are an expert data analyst tasked with suggesting intelligent follow-up questions.
Given the context, generate three follow-up questions.

### Database Schema:
{formatted_schema}

### Recent User Question:
"{question}"

### Generated SQL Query:
```sql
{sql_query}
```

### Sample Query Results:
```json
{formatted_results}
```

### Recent Conversation History:
{formatted_history}

### INSTRUCTIONS:
Generate three follow-up questions, one for each category:
1. sql_retrieval: A question for specific data points.
2. visualization: A question that would lead to a chart.
3. insights: A question seeking patterns or higher-level analysis.

Your output must be a JSON object with EXACTLY this structure:
{{
  "typed_recommendations": {{
    "sql_retrieval": "your recommended question here",
    "visualization": "your recommended question here",
    "insights": "your recommended question here"
  }}
}} """


# ==============================================================================
# VI. GEMINI LLM QUERY FUNCTIONS
# ==============================================================================

def query_gemini_text(prompt: str) -> str:
    """Queries a Gemini text model and returns the text response."""
    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        print("Querying Gemini for text generation...")
        response = model.generate_content(prompt, generation_config={"temperature": 0.1})
        return response.text.strip()
    except google_exceptions.GoogleAPICallError as e:
        print(f"Google API Call Error: {e}")
        raise HTTPException(status_code=502, detail=f"A Google API error occurred: {e}")
    except Exception as e:
        print(f"Gemini text generation error: {e}")
        raise HTTPException(status_code=500, detail=f"LLM text generation error: {e}")


def query_gemini_json(prompt: str) -> Dict:
    """Queries a Gemini model expected to return a JSON object."""
    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        print("Querying Gemini for JSON generation...")
        response = model.generate_content(
            prompt,
            generation_config={"response_mime_type": "application/json", "temperature": 0.0}
        )
        return json.loads(response.text)
    except google_exceptions.GoogleAPICallError as e:
        print(f"Google API Call Error: {e}")
        raise HTTPException(status_code=502, detail=f"A Google API error occurred: {e}")
    except Exception as e:
        print(f"Gemini JSON generation error: {e}")
        raise HTTPException(status_code=500, detail=f"LLM JSON generation error: {e}")


# ==============================================================================
# VII. CORE LOGIC & HELPER FUNCTIONS
# ==============================================================================

def create_interactive_chart(results: list[dict], chart_metadata: dict) -> go.Figure:
    """Creates a Plotly chart Figure from data and metadata."""
    chart_type = chart_metadata.get("chart_type")
    title = chart_metadata.get("title", "Chart")

    fig = go.Figure()

    if not results:
        fig.update_layout(title=title, annotations=[{"text": "No data available for visualization.", "showarrow": False}])
        return fig

    df = pd.DataFrame(results)

    try:
        if chart_type == "bar":
            fig = px.bar(df, x=chart_metadata.get("x_axis_column"), y=chart_metadata.get("y_axis_column"), title=title)
        elif chart_type == "pie":
            fig = px.pie(df, names=chart_metadata.get("label_column"), values=chart_metadata.get("value_column"), title=title)
        elif chart_type == "line":
            fig = px.line(df, x=chart_metadata.get("x_axis_column"), y=chart_metadata.get("y_axis_column"), title=title, markers=True)
        elif chart_type in ["histogram", "violin", "box"]:
            value_col = chart_metadata.get("value_column")
            if chart_type == "histogram": fig = px.histogram(df, x=value_col, title=title)
            elif chart_type == "violin": fig = px.violin(df, y=value_col, title=title, box=True)
            elif chart_type == "box": fig = px.box(df, y=value_col, title=title)
        else:
            fig.update_layout(title=f"Unsupported chart type: {chart_type}")

        fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        return fig

    except Exception as e:
        print(f"Error creating Plotly chart: {e}")
        fig.update_layout(title=f"Error generating chart: {e}")
        return fig
    
# ==============================================================================
# VIII. MAIN PIPELINE FUNCTIONS
# ==============================================================================

async def run_sql_pipeline(question: str, user_id: str, session_id: str):
    """Orchestrates the Text-to-SQL and SQL-NLG generation pipeline."""
    try:
        if "current" not in schema_cache:
            return {"error": "Not connected to a database. Please connect first.", "pipeline": "sql"}

        schema = schema_cache["current"]["schema"]
        raw_history = get_session_history(user_id, session_id)
        history = parse_history_items(raw_history)


        sql_prompt = generate_sql_query_prompt(schema, question, history)
        sql_json = query_gemini_json(sql_prompt)
        sql_query = sql_json.get("sql_query")
        if not sql_query:
            return {"error": "Failed to generate a valid SQL query.", "pipeline": "sql", "question": question}

        results = execute_sql_query_mysql(db_pool, sql_query)
        save_interaction_to_history(user_id, session_id, question, sql_query, "sql")
        raw_updated_history = get_session_history(user_id, session_id)
# Add this line to parse the history from strings to dictionaries
        updated_history = parse_history_items(raw_updated_history) 

        loop = asyncio.get_running_loop()
        nlg_prompt = generate_sql_nlg_prompt(question, sql_query, results)
# Now pass the correctly parsed history
        recommendations_prompt = generate_typed_recommendations_prompt(schema, question, sql_query, results, updated_history)

        nlg_task = loop.run_in_executor(None, query_gemini_text, nlg_prompt)
        recommendations_task = loop.run_in_executor(None, query_gemini_json, recommendations_prompt)

        api_results = await asyncio.gather(nlg_task, recommendations_task, return_exceptions=True)

        nlg_response = "Error generating response text." if isinstance(api_results[0], Exception) else api_results[0]
        typed_recommendations = {} if isinstance(api_results[1], Exception) else api_results[1].get("typed_recommendations", {})
        serializable_results = simplejson.loads(
            simplejson.dumps(results, default=str, use_decimal=True, ignore_nan=True)
        )
        return {
            "pipeline": "sql", "question": question, "sql_query": sql_query,
            "results": serializable_results, # Return the sanitized data
            "nlg_response": nlg_response,
            "typed_recommendations": typed_recommendations,
        }
    except Exception as e:
        print(f"Critical error in SQL pipeline: {e}")
        return {"error": f"A critical error occurred: {str(e)}", "pipeline": "sql", "question": question}


async def run_visualization_pipeline(question: str, user_id: str, session_id: str):
    """Orchestrates the data retrieval and metadata generation for visualizations."""
    try:
        if "current" not in schema_cache:
            return {"error": "Not connected to a database. Please connect first.", "pipeline": "visualization"}

        schema = schema_cache["current"]["schema"]
        raw_history = get_session_history(user_id, session_id)
        history = parse_history_items(raw_history)


        sql_prompt = generate_sql_query_prompt(schema, question, history)
        sql_json = query_gemini_json(sql_prompt)
        sql_query = sql_json.get("sql_query")
        if not sql_query:
            return {"error": "Failed to generate SQL for visualization.", "pipeline": "visualization"}

        results = execute_sql_query_mysql(db_pool, sql_query)
        save_interaction_to_history(user_id, session_id, question, sql_query, "visualization")
        raw_updated_history = get_session_history(user_id, session_id)
# Add this line to parse the history from strings to dictionaries
        updated_history = parse_history_items(raw_updated_history)

        chart_meta_prompt = generate_chart_metadata_prompt(question, sql_query, results)
        chart_metadata = query_gemini_json(chart_meta_prompt)

        loop = asyncio.get_running_loop()
        summary_prompt = generate_visualization_nlg_prompt(question, chart_metadata, results)
# Now pass the correctly parsed history
        recommendations_prompt = generate_typed_recommendations_prompt(
        schema, question, sql_query, results, updated_history
        )
        nlg_task = loop.run_in_executor(None, query_gemini_text, summary_prompt)
        recommendations_task = loop.run_in_executor(None, query_gemini_json, recommendations_prompt)
        api_results = await asyncio.gather(nlg_task, recommendations_task, return_exceptions=True)

        summary_text = "Error generating summary." if isinstance(api_results[0], Exception) else api_results[0]
        typed_recommendations = {} if isinstance(api_results[1], Exception) else api_results[1].get("typed_recommendations", {})
        serializable_results = simplejson.loads(
            simplejson.dumps(results, default=str, use_decimal=True, ignore_nan=True)
        )
        return {
            "pipeline": "visualization", "question": question, "sql_query": sql_query,
            "results": serializable_results, # Return the sanitized data
            "chart_metadata": chart_metadata,
            "nlg_response": summary_text, "typed_recommendations": typed_recommendations
        }
    except Exception as e:
        print(f"Critical error in visualization pipeline: {e}")
        return {"error": f"A critical error occurred: {str(e)}", "pipeline": "visualization", "question": question}


async def run_insights_pipeline(question: str, user_id: str, session_id: str):
    """Orchestrates the hierarchical insights generation pipeline."""
    try:
        if "current" not in schema_cache:
            return {"error": "Not connected to a database. Please connect first.", "pipeline": "insights"}

        schema = schema_cache["current"]["schema"]
        raw_history = get_session_history(user_id, session_id)
        history = parse_history_items(raw_history)


        sql_prompt = generate_sql_query_prompt(schema, question, history)
        sql_json = query_gemini_json(sql_prompt)
        sql_query = sql_json.get("sql_query")
        if not sql_query:
            return {"error": "Failed to generate SQL for insights.", "pipeline": "insights"}

        results = execute_sql_query_mysql(db_pool, sql_query)
        save_interaction_to_history(user_id, session_id, question, sql_query, "insights")
        raw_updated_history = get_session_history(user_id, session_id)
# Add this line to parse the history from strings to dictionaries
        updated_history = parse_history_items(raw_updated_history)

# Now pass the correctly parsed history
        recommendations_prompt = generate_typed_recommendations_prompt(schema, question, sql_query, results, updated_history)
        if not results:
            recommendations_json = query_gemini_json(recommendations_prompt)
            return {
                "pipeline": "insights", "question": question, "sql_query": sql_query,
                "nlg_response": "No data was found for your question.",
                "typed_recommendations": recommendations_json.get("typed_recommendations", {})
            }

        df = pd.DataFrame(results)
        CHUNK_SIZE = 500
        loop = asyncio.get_running_loop()
        recommendations_task = loop.run_in_executor(None, query_gemini_json, recommendations_prompt)

        if len(df) <= CHUNK_SIZE:
            prompt = generate_insight_chunk_prompt(df, question)
            insight_task = loop.run_in_executor(None, query_gemini_text, prompt)
            final_summary_res, recommendations_res = await asyncio.gather(insight_task, recommendations_task, return_exceptions=True)
        else:
            chunks = [df.iloc[i:i + CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]
            chunk_insight_tasks = [loop.run_in_executor(None, query_gemini_text, generate_insight_chunk_prompt(c, question)) for c in chunks]
            chunk_insights_results = await asyncio.gather(*chunk_insight_tasks, return_exceptions=True)
            chunk_insights = [res for res in chunk_insights_results if not isinstance(res, Exception)]

            merge_prompt = generate_insight_summary_prompt(chunk_insights, question)
            merge_task = loop.run_in_executor(None, query_gemini_text, merge_prompt)
            final_summary_res, recommendations_res = await asyncio.gather(merge_task, recommendations_task, return_exceptions=True)

        final_summary = "Error generating insight." if isinstance(final_summary_res, Exception) else final_summary_res
        typed_recommendations = {} if isinstance(recommendations_res, Exception) else recommendations_res.get("typed_recommendations", {})

        return {
            "pipeline": "insights", "question": question, "sql_query": sql_query,
            "nlg_response": final_summary, "typed_recommendations": typed_recommendations
        }
    except Exception as e:
        print(f"Critical error in insights pipeline: {e}")
        return {"error": f"A critical error occurred: {str(e)}", "pipeline": "insights", "question": question}


# ==============================================================================
# IX. FASTAPI ENDPOINTS
# ==============================================================================

@app.get("/")
def read_index():
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    raise HTTPException(status_code=404, detail=f"Index file not found: {index_path}")


@app.get("/main.html")
def serve_main():
    main_path = STATIC_DIR / "main.html"
    if main_path.exists():
        return FileResponse(str(main_path))
    raise HTTPException(status_code=404, detail=f"Main file not found: {main_path}")


@app.post("/connect-mysql")
async def connect_mysql_endpoint(
    host: str = Form(...), port: int = Form(3306),
    db_name: str = Form(...), username: str = Form(...), password: str = Form(...)
):
    global db_pool
    try:
        if db_pool:
            db_pool.close()
        db_pool = MySQLConnectionPool(
            pool_name="mypool", pool_size=5,
            host=host, port=port, user=username, password=password, database=db_name
        )
        schema_json = extract_mysql_schema_json(db_pool)
        # encrypt_text may be None if the security module couldn't be imported
        # (for example during some dev setups). Fall back to plaintext to avoid
        # NoneType errors while preserving the ability to encrypt when available.
        if callable(encrypt_text):
            encrypted_password = encrypt_text(password)
        else:
            # Bypass encryption safely (development fallback)
            encrypted_password = password
        schema_cache["current"] = {
            "connection": {"host": host, "port": port, "user": username, "password": encrypted_password, "database": db_name},
            "schema": schema_json
        }
        return JSONResponse(
            content={"message": "Connection successful"},
            headers={"HX-Redirect": "/main.html"}
        )
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Connection Failed: {err}")


@app.post("/ask")
async def ask_endpoint(
    request: Request,
    question: str = Form(...),
    pipeline: Optional[str] = Form(None),
    mute_audio: bool = Form(False)
):
    user_id = request.state.user_id
    session_id = request.state.session_id
    forced_pipeline = pipeline
    
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    if "current" not in schema_cache:
        raise HTTPException(status_code=400, detail="Not connected to a database.")

    # Cancel any previous task for this session
    task_key = f"{user_id}:{session_id}"
    if task_key in active_tasks and not active_tasks[task_key].done():
        active_tasks[task_key].cancel()

    # Decide pipeline
    if not forced_pipeline:
        try:
            intent_prompt = generate_intent_classification_prompt(question)
            intent_raw = query_gemini_text(intent_prompt).lower()
            if "visual" in intent_raw:
                forced_pipeline = "visualization"
            elif "insight" in intent_raw:
                forced_pipeline = "insights"
            else:
                forced_pipeline = "sql"
        except Exception as e:
            raise HTTPException(status_code=500, detail="Could not determine user intent.")
    
    # Create the pipeline coroutine
    if forced_pipeline == "sql":
        pipeline_coro = run_sql_pipeline(question, user_id, session_id)
    elif forced_pipeline == "visualization":
        pipeline_coro = run_visualization_pipeline(question, user_id, session_id)
    elif forced_pipeline == "insights":
        pipeline_coro = run_insights_pipeline(question, user_id, session_id)
    else:
        raise HTTPException(status_code=400, detail="Invalid pipeline specified.")

    # Store the task
    loop = asyncio.get_running_loop()
    task = loop.create_task(pipeline_coro)
    active_tasks[task_key] = task

    try:
        result = await task
    except asyncio.CancelledError:
        return JSONResponse({"pipeline": forced_pipeline, "status": "cancelled"}, status_code=200)

    # Only run TTS if not muted
    if not mute_audio and result.get("nlg_response"):
        audio_response = await text_to_speech_async(result["nlg_response"])
        result["audio_response"] = audio_response

    return JSONResponse(content=result, status_code=(200 if "error" not in result else 500))

@app.post("/stop")
async def stop_pipeline(request: Request):
    user_id = request.state.user_id
    session_id = request.state.session_id
    task_key = f"{user_id}:{session_id}"
    
    task = active_tasks.get(task_key)
    if task and not task.done():
        task.cancel()
        return {"status": "stopping"}
    return {"status": "no_active_task"}


@app.post("/clear-history")
async def clear_history_endpoint(request: Request):
    """Clears the conversation history for a given user and session."""
    user_id = request.state.user_id
    session_id = request.state.session_id
    clear_session_history(user_id, session_id)
    return {"message": "History cleared successfully."}


@app.get("/history")
async def get_history_endpoint(request: Request):
    """Retrieves the last 20 interactions for the current session."""
    user_id = request.state.user_id
    session_id = request.state.session_id
    # get_session_history returns a list of JSON strings.
    history = get_session_history(user_id, session_id, n=20)
    return JSONResponse(content={"history": history})

@app.get("/health")
async def health_check():
    """
    Performs a health check and returns the status of the API and its dependencies.
    """
    # Check the status of each core dependency
    db_status = "configured" if db_pool is not None and "current" in schema_cache else "not_configured"
    redis_status = "ok" if redis_available() else "unavailable"
    
    # CORRECTED: Check for the new unified authentication method.
    gemini_status = "configured" if os.getenv("GOOGLE_API_KEY") else "not_configured"
    # The service is considered healthy if its core non-DB dependencies are up.
    # The DB is a user-configured dependency, so its absence doesn't mean the service is down.
    is_healthy = redis_status == "ok" and gemini_status == "configured"
    overall_status = "ok" if is_healthy else "service_unavailable"
    
    response_payload = {
        "status": overall_status,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "dependencies": {
            "database": db_status,
            "redis": redis_status,
            "gemini_api": gemini_status
        }
    }

    # Return HTTP 200 if the service is running, even if a dependency has an issue,
    # as is common practice for health checks that report status in their body.
    return JSONResponse(content=response_payload, status_code=200)

@app.get("/health-ui", response_class=HTMLResponse)
async def get_health_ui():
    """Serves the frontend page for displaying the health status."""
    health_path = STATIC_DIR / "health.html"
    if health_path.exists():
        return FileResponse(str(health_path))
    raise HTTPException(status_code=404, detail=f"Health UI not found: {health_path}")

for route in app.router.routes:
    methods = getattr(route, "methods", None)
    path = getattr(route, "path", None) or getattr(route, "prefix", None)
    name = getattr(route, "name", None) or route.__class__.__name__
    print(f"ROUTE: methods={methods} path={path} name={name}")

# Application Entry Point
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)