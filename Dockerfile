FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends git openssh-client && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/
COPY templates/ templates/

RUN pip install --no-cache-dir -e .

EXPOSE 8000

CMD ["scripthut", "--host", "0.0.0.0"]
