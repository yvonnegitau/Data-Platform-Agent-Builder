FROM python:3.11-slim

WORKDIR /app

# Install uv for fast installs
RUN pip install --no-cache-dir uv

# Copy and install MCP server dependencies
COPY mcp_server/requirements.txt ./requirements.txt
RUN uv pip install --system -r requirements.txt

# Install mcpo — the MCP → OpenAI-compatible HTTP bridge
RUN uv pip install --system mcpo

# Copy MCP server code
COPY mcp_server/ ./mcp_server/

# Run from inside mcp_server so relative imports (config.py, tools/, etc.) resolve correctly
WORKDIR /app/mcp_server

# mcpo starts our MCP server as a subprocess and proxies tool calls over HTTP
CMD ["mcpo", "--port", "8000", "--", "python", "server.py"]
