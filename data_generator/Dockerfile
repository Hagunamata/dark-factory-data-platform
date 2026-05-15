FROM python:3.12-slim

WORKDIR /app

# Install dependencies first for better layer caching
COPY data_generator/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Module code is mounted as a volume in docker-compose for fast iteration
# during development. For production builds, COPY it in instead.

CMD ["python", "-m", "data_generator.bootstrap"]
