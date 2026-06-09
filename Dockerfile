FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/app/data \
    BOT_LOCK_FILE=/app/data/dekor_autoposting.lock \
    RUN_LOG_PATH=/app/data/run_log.json

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

RUN mkdir -p /app/data

CMD ["python", "-m", "src.dekor_bot.main"]
