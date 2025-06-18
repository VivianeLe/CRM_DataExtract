FROM openjdk:17-slim

# --- Install only essential system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    python3 \
    python3-pip \
    gcc \
    g++ \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
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