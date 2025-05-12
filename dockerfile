# --- Base image with Java & Python
FROM openjdk:11-jdk-slim

# --- Install Python & pip
RUN apt-get update && apt-get install -y python3 python3-pip

# --- Set working directory
WORKDIR /app

# --- Copy source code
COPY . .

# --- Install Python dependencies
RUN pip3 install -r requirements.txt

# --- Expose Streamlit port
EXPOSE 8501

# --- Run Streamlit on container start
CMD ["streamlit", "run", "app.py", "--server.headless=true", "--server.port=8501", "--server.address=0.0.0.0"]
