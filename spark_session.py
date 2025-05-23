import os
from pyspark.sql import SparkSession
import streamlit as st
import glob

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

@st.cache_resource
def init_spark():
    # spark = SparkSession.builder \
    #     .appName("CRM Data Extract") \
    #     .master(f"local[{os.cpu_count()}]") \
    #     .config("spark.jars", "/app/jars/mssql-jdbc-12.2.0.jre11.jar,/app/jars/msal4j-1.13.8.jar,/app/jars/slf4j-api-1.7.36.jar") \
    #     .config("spark.driver.memory", "12g") \
    #     .config("spark.executor.memory", "12g") \
    #     .config("spark.sql.shuffle.partitions", "28") \
    #     .config("spark.default.parallelism", "28") \
    #     .config("spark.local.dir", "/tmp/spark-temp") \
    #     .getOrCreate()

    spark = SparkSession.builder \
        .appName("CRM Data Extract") \
        .master(f"local[{os.cpu_count()}]") \
        .config("spark.jars.packages", ",".join([
            "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre11",
            "com.microsoft.azure:msal4j:1.13.8",
            "org.slf4j:slf4j-api:1.7.36"
        ])) \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "12g") \
        .config("spark.sql.shuffle.partitions", "28") \
        .config("spark.default.parallelism", "28") \
        .config("spark.local.dir", "C:/spark-temp") \
        .getOrCreate()
    return spark

def markdown():
    st.markdown("""
    <style>
    /* --- Banner custom --- */
    .gold-banner {
        background-color: #FFD700;
        padding: 12px;
        border-radius: 10px;
        text-align: center;
        margin-top: 10px;
        margin-bottom: 20px;
    }

    .gold-banner h2 {
        color: black !important;
        font-size: 28px;
        font-weight: bold;
    }

    /* --- Button styling --- */
    .stButton>button,
    button[kind="primary"] {
        background-color: #FFD700 !important;
        color: black !important;
        font-weight: bold;
        border: none;
        border-radius: 8px;
        padding: 0.5em 1.5em;
    }
                
    .stDownloadButton>button {
    background-color: #FFD700;
    color: black;
    font-weight: bold;
    border: none;
    border-radius: 8px;
    padding: 0.5em 1.5em;
}

.stDownloadButton>button:hover {
    background-color: #e6c200;
    color: black;
    </style>
    """, unsafe_allow_html=True)

def save_csv_file(data, file_name):
    output_dir = "/tmp/output_csv"
    output_file = f"/tmp/{file_name}.csv"
    data.write.mode("overwrite").option("header", True).csv(output_dir)

    # combine into 1 file
    part_files = sorted(glob.glob(f"{output_dir}/part-*.csv"))
    with open(output_file, "w", encoding="utf-8") as f_out:
        for i, part_file in enumerate(part_files):
            with open(part_file, "r", encoding="utf-8") as f_in:
                lines = f_in.readlines()
                if i == 0:
                    f_out.writelines(lines)
                else:
                    f_out.writelines(lines[1:])

    with open(output_file, "rb") as f:
        st.download_button(
            label="📥 Download CSV",
            data=f,
            file_name=f"{file_name}",
            mime="text/csv"
        )