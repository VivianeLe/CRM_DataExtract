import os
from pyspark.sql import SparkSession
import streamlit as st
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

@st.cache_resource
def init_spark():
    spark = SparkSession.builder \
        .appName("MySQL Data Transform") \
        .config("spark.default.parallelism", "14")\
        .master(f"local[{os.cpu_count()}]") \
        .config("spark.jars", r"C:\sqljdbc_12.10\enu\jars\mssql-jdbc-12.10.0.jre11.jar") \
        .config("spark.local.dir", r"C:\spark-temp") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "14")\
        .getOrCreate()
    return spark

def markdown():
    st.markdown("""
    <style>

    /* Banner custom */
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

    /* Text and button */
    .stButton>button {
        background-color: #FFD700;
        color: black;
        font-weight: bold;
        border: none;
        border-radius: 6px;
        padding: 0.5em 1.5em;
    }

    .stButton>button:hover {
        background-color: #e6c200;
        color: black;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
<style>
button[kind="primary"] {
    background-color: #FFD700 !important;
    color: black !important;
    font-weight: bold;
    border-radius: 8px;
}
</style>
""", unsafe_allow_html=True)
