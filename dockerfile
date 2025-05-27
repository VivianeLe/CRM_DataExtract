FROM openjdk:17-slim

# --- Install only essential system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    unixodbc \
    unixodbc-dev \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# --- Set working directory
WORKDIR /app

# --- Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

# --- Copy source code
COPY . .
RUN mkdir -p /app/jars
COPY jars/ /app/jars/

# --- Set Spark JDBC JAR path
ENV SPARK_JDBC_JAR=/app/jars/mssql-jdbc-12.10.0.jre11.jar

# --- Expose Streamlit port
EXPOSE 8501

# --- Run Streamlit app
CMD ["streamlit", "run", "app.py", "--server.headless=true", "--server.port=8501", "--server.address=0.0.0.0"]