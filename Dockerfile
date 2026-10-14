# Builds the pipeline on the sample data and serves the dashboard and API.
#   docker build -t grid-data-pipeline .
#   docker run -p 8000:8000 grid-data-pipeline
FROM python:3.12-slim

WORKDIR /app
COPY gridpipe ./gridpipe
COPY data ./data

RUN useradd --create-home app && chown -R app /app
USER app

RUN python -m gridpipe --db grid.db run

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health')"
CMD ["python", "-m", "gridpipe", "--db", "grid.db", "serve", "--host", "0.0.0.0", "--port", "8000"]
