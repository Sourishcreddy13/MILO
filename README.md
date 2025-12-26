# Intelligent Data Assistant API

An AI-powered FastAPI application that translates natural language questions into SQL queries, generates interactive visualizations, and provides intelligent data insights using Google's Gemini AI.

## Features

- **Natural Language to SQL**: Ask questions in plain English and get SQL queries automatically generated
- **Smart Intent Classification**: Automatically determines whether to retrieve data, create visualizations, or generate insights
- **Interactive Visualizations**: Creates dynamic charts (bar, pie, line, histogram, violin, box plots) using Plotly
- **Hierarchical Insights**: Analyzes large datasets in chunks and synthesizes high-level summaries
- **Text-to-Speech**: Converts AI responses to audio for hands-free interaction
- **Conversation History**: Maintains context across sessions using Redis
- **Typed Recommendations**: Suggests relevant follow-up questions categorized by type
- **Real-time Streaming**: WebSocket support for live updates

## Prerequisites

- Python 3.8+
- MySQL database
- Redis server
- FFmpeg (required for audio functionality)
- Google Gemini API key

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd intelligent-data-assistant
```

### 2. Install FFmpeg

FFmpeg is required for text-to-speech audio processing.

**Windows:**
- Download from [ffmpeg.org](https://ffmpeg.org/download.html)
- Extract and add to system PATH
- Or use chocolatey: `choco install ffmpeg`

**macOS:**
```bash
brew install ffmpeg
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install ffmpeg
```

**Verify installation:**
```bash
ffmpeg -version
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

Required packages include:
- fastapi
- uvicorn
- mysql-connector-python
- redis
- pandas
- google-generativeai
- plotly
- python-dotenv
- simplejson
- pydantic

### 4. Set Up Environment Variables

Create a `.env` file in the project root:

```env
# Google Gemini API
GOOGLE_API_KEY=your_gemini_api_key_here

# Redis Configuration
REDIS_URL=redis://localhost:6379/0

# Optional: Encryption keys (if using the security module)
ENCRYPTION_KEY=your_encryption_key_here
```

### 5. Set Up Redis

**Using Docker:**
```bash
docker run -d -p 6379:6379 redis
```

**Or install locally:**
- **macOS:** `brew install redis && brew services start redis`
- **Linux:** `sudo apt install redis-server && sudo systemctl start redis`
- **Windows:** Download from [Redis Windows](https://github.com/microsoftarchive/redis/releases)

### 6. Prepare Your MySQL Database

Ensure you have a MySQL database with tables and data ready to query. The application will extract the schema automatically upon connection.

## Project Structure

```
intelligent-data-assistant/
├── backend/
│   ├── new.py              # Main application file
│   ├── audio.py            # Text-to-speech module
│   └── security.py         # Encryption utilities (optional)
├── static/
│   ├── index.html          # Landing/connection page
│   ├── main.html           # Main chat interface
│   └── health.html         # Health status UI
├── .env                    # Environment variables
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## Usage

### Starting the Server

```bash
python backend/new.py
```

Or with uvicorn directly:
```bash
uvicorn backend.new:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at `http://localhost:8000`

### Connecting to a Database

1. Navigate to `http://localhost:8000`
2. Fill in your MySQL connection details:
   - Host
   - Port (default: 3306)
   - Database Name
   - Username
   - Password
3. Click "Connect"

### Asking Questions

Once connected, you can ask questions in three modes:

**SQL Retrieval:**
```
"Show me the top 10 customers by revenue"
"How many orders were placed last month?"
"List all products in the electronics category"
```

**Visualization:**
```
"Draw a bar chart of sales by region"
"Plot revenue trends over the past year"
"Show me a pie chart of product categories"
```

**Insights:**
```
"What are the key trends in customer behavior?"
"Summarize our sales performance"
"Tell me about our top-performing products"
```

## API Endpoints

### Core Endpoints

- `POST /connect-mysql` - Connect to a MySQL database
- `POST /ask` - Ask a natural language question
- `POST /stop` - Stop the current pipeline execution
- `POST /clear-history` - Clear conversation history
- `GET /history` - Retrieve conversation history
- `GET /health` - Health check endpoint
- `GET /health-ui` - Health status UI

### Request Example

```bash
curl -X POST "http://localhost:8000/ask" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "question=Show me top 5 customers&pipeline=sql&mute_audio=false"
```

### Response Example

```json
{
  "pipeline": "sql",
  "question": "Show me top 5 customers",
  "sql_query": "SELECT name, total_revenue FROM customers ORDER BY total_revenue DESC LIMIT 5",
  "results": [...],
  "nlg_response": "Here are your top 5 customers by revenue...",
  "typed_recommendations": {
    "sql_retrieval": "What is the average order value for these customers?",
    "visualization": "Show me a chart of revenue by customer",
    "insights": "What patterns do you see in top customer behavior?"
  },
  "audio_response": "base64_encoded_audio..."
}
```

## Configuration

### Pipeline Types

The system automatically classifies questions into three pipelines:

1. **SQL Pipeline**: Direct data retrieval
2. **Visualization Pipeline**: Chart generation
3. **Insights Pipeline**: Pattern analysis and summaries

You can force a specific pipeline by setting the `pipeline` parameter.

### Gemini Models

The application uses `gemini-2.5-flash` for all LLM operations. You can modify this in the `query_gemini_text()` and `query_gemini_json()` functions.

### Chart Types

Supported visualization types:
- Bar charts
- Pie charts
- Line charts
- Histograms
- Violin plots
- Box plots

## Troubleshooting

### Common Issues

**Database Connection Fails:**
- Verify MySQL credentials
- Check if MySQL server is running
- Ensure the database exists

**Redis Connection Error:**
- Start Redis server: `redis-server`
- Check Redis URL in `.env` file

**FFmpeg Not Found:**
- Ensure FFmpeg is installed and in system PATH
- Restart terminal after installation

**Gemini API Errors:**
- Verify `GOOGLE_API_KEY` in `.env`
- Check API quota and billing
- Ensure API is enabled in Google Cloud Console

**Audio Not Working:**
- Check FFmpeg installation
- Verify `audio.py` module exists
- Try with `mute_audio=true` to bypass audio

### Debug Mode

Enable detailed logging by running:
```bash
uvicorn backend.new:app --log-level debug
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Structure

The application follows a modular pipeline architecture:
1. Intent classification
2. SQL generation
3. Query execution
4. Result processing
5. Response generation (NLG, charts, insights)
6. Recommendation generation

## Security Notes

- Database passwords are encrypted at rest using the `security` module
- Session data is stored with HttpOnly cookies
- Redis keys expire after 24 hours
- SQL queries are generated by AI (monitor for injection risks)

## Performance

- Connection pooling for MySQL (5 connections)
- Async operations for parallel LLM calls
- Chunked processing for large datasets (500 rows per chunk)
- Redis caching for schema and history

## License

[Your License Here]

## Contributing

Contributions welcome! Please submit pull requests or open issues for bugs and feature requests.

## Support

For issues or questions:
- Open a GitHub issue
- Check the health endpoint: `http://localhost:8000/health-ui`
- Review logs in the console output

## Acknowledgments

- Built with FastAPI
- Powered by Google Gemini AI
- Visualizations by Plotly
- Audio processing with FFmpeg
