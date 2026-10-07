FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Shanghai

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app.py /app/app.py
COPY fetcher.py /app/fetcher.py
COPY quota_watcher.py /app/quota_watcher.py
COPY templates /app/templates

EXPOSE 5000

CMD ["python", "app.py"]
