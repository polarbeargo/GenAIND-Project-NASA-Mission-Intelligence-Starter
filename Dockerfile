FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --group monitoring-postgres

COPY . .

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8000"]
