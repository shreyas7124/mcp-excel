# mcp-excel
Parse the excel files and then will provide the schema


# MCP Excel Integration

## Project Overview
This project enables ingestion of Excel files into a Model Context Protocol (MCP) server, which can be queried interactively via an AI agent like Claude Desktop. It supports:

- Extracting Excel sheet data with optional schema mapping.
- Calling a Groq LLM to convert tabular and Raw text to structured JSON.
- Pushing parsed data into an MCP server.
- Querying Excel data through the MCP server tools integrated with Claude Desktop.

## Features
- Schema-based or best-effort Excel extraction.
- Robust JSON parsing of LLM output.
- MCP server exposing tools for sheet listing, data description, filtering, and aggregation.
- Integration with Claude Desktop as an MCP client.

## Setup Instructions

### 1. Environment Variables
Create a `.env` file in your project root with the following variables:

GROQ_API_KEY="your_groq_api_key_here"
MCP_ENDPOINT="http://localhost:8000" # Or your MCP server endpoint
MCP_API_KEY="" # Fill if your MCP server requires API key auth



### 2. Install Python Dependencies
Run: pip install pandas openpyxl python-dotenv requests fastapi uvicorn


### 3. Running the MCP Server
Start the MCP server (which hosts your Excel query tools):


This runs the MCP server with an HTTP interface by default on port 8000.

python mcp_server.py --http

### 4. Pushing Excel Data to MCP Server
Use the Excel ingestion script to process and push Excel files:

python mcp_excel2.py /path/to/file.xlsx --schema schema.json --model <model-name> --push-to-mcp


### 5. Connect Claude Desktop to MCP Server
Configure Claude Desktop to connect to your MCP server in `claude_desktop_config.json`, e.g.:{
"mcpServers": {
"mcp_server": {
"command": "path_to_python",
"args": ["path_to/mcp_server.py"],
"transport": "stdio"
}
}
}


Restart Claude Desktop after this change.

## Usage Example in Claude Desktop
- Load files: `load_file("absolute_path_to_excel.xlsx")`
- List sheets: `list_sheets()`
- Query data: `get_rows(sheet="Sheet1", q="some query")`
- Aggregate: `aggregate(sheet="Sheet1", group_by="Category", agg_col="Amount", agg_func="sum")`

## Notes
- Modify paths and keys as per your environment.
- MCP server supports both stdio (for Claude Desktop) and HTTP modes.
- This project is structured for extensibility and robust data processing.





