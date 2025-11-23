FROM python:3.10-slim

# Install Java (Spark requirement)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

# Set JAVA_HOME dynamically
RUN echo "JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))" >> /etc/environment

ENV PATH="/usr/lib/jvm/default-java/bin:${PATH}"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Prepare app folder
WORKDIR /app
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

CMD ["/bin/bash", "/app/entrypoint.sh"]
