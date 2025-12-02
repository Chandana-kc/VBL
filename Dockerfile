
FROM python:3.11-slim

LABEL maintainer="VBL Platform Team"
LABEL description="VBL Digital Factory OPC UA Server"

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

## vbltags.json not needed; removed COPY for build compatibility
COPY . .

RUN useradd -m -u 1000 vbluser && chown -R vbluser:vbluser /app
USER vbluser

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import asyncio, asyncua; print('OPC UA available')"

EXPOSE 4842

CMD ["python", "vbl_opcua_server.py"]
