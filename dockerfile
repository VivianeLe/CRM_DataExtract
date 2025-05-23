FROM openjdk:17-jdk-slim

# --- Install Python, pip, and system dependencies for ODBC and build
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    gcc \
    g++ \
    unixodbc \
    unixodbc-dev \
    libpq-dev \
    procps \
    && rm -rf /var/lib/apt/lists/*

# --- Set working directory
WORKDIR /app

# --- Copy source code into container
COPY . .

# --- Install Python dependencies
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt

# --- (Optional) If you need to add the MSSQL JDBC JAR for Spark
ENV SPARK_JDBC_JAR=/app/mssql-jdbc-12.10.0.jre11.jar

# --- Expose Streamlit's default port
EXPOSE 8501

# --- Start the app
CMD ["streamlit", "run", "app.py", "--server.headless=true", "--server.port=8501", "--server.address=0.0.0.0"]
