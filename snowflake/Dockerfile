FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    SNOWFLAKE_PORT=8082 \
    SNOWFLAKE_USER=fake \
    SNOWFLAKE_PASSWORD=snow \
    SNOWFLAKE_ACCOUNT=fakesnow \
    SNOWFLAKE_DATABASE=DEMO_DB \
    SNOWFLAKE_SCHEMA=PUBLIC \
    SNOWFLAKE_WAREHOUSE=DEMO_WH \
    SNOWFLAKE_TABLE=CUSTOMERS \
    SNOWFLAKE_PROTOCOL=http \
    SNOWFLAKE_INTERNAL_HOST=127.0.0.1

RUN pip install --no-cache-dir "fakesnow[server]==0.10.1" snowflake-connector-python==3.17.3

COPY bootstrap.py /bootstrap.py
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 8082

CMD ["/entrypoint.sh"]
