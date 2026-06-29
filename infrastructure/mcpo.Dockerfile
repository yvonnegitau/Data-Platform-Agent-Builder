FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY mcp_server/requirements.txt ./requirements.txt
RUN uv pip install --system -r requirements.txt

COPY mcp_server/ ./mcp_server/

WORKDIR /app/mcp_server

CMD ["python", "server.py", "http"]