import os 
from pyspark.sql.functions import *
import tempfile
from dotenv import load_dotenv
import streamlit as st
import pyodbc

load_dotenv()
db_password = os.environ.get("Azure_DB_pass")
database= os.environ.get("DATABASE")
user= os.environ.get("USER")

if db_password is None:
    db_password = input("Please enter database password: ")

jdbc_url = (
    "jdbc:sqlserver://sql-nlbi-prd-uaen-01.database.windows.net:1433;database={};" 
    "user={}@sql-nlbi-prd-uaen-01;password={};" 
    "encrypt=true;trustServerCertificate=true;" 
    "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
).format(database, user, db_password)

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

conn_str = (
           " Driver={ODBC Driver 17 for SQL Server};"
           "Server=tcp:sql-nlbi-prd-uaen-01.database.windows.net,1433;"
           f"Database={database};Uid={user};"
           f"Pwd={db_password};Encrypt=yes;"
           "TrustServerCertificate=no;Connection Timeout=30;"
        )

def read_csv_spark(spark, uploaded_file):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(uploaded_file.getbuffer())
        tmp_path = tmp.name

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(tmp_path)

    return df

def run_query(query):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        st.error(f"❌ Failed to run {query}:\n{e}")

def update_dim_user(df, table, query):
    run_query(f"TRUNCATE TABLE {table}")
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("driver", driver) \
        .mode("append") \
        .save()
    run_query(query)
    # st.success(f"✅ {table} data is updated")

def query_data(spark, user_ids_df, start_date, end_date):
    # This function check activity of users after receiving marketing message
    dim_series = run_select_query(spark, """select SeriesNo as Series_No, 
                        GameType from dbo.dim_series""")
    query = f"""
        SELECT User_ID, Order_ID, Series_No, Entries, Turnover, Prize
         FROM dbo.fact_orders o
         WHERE Creation_date BETWEEN '{start_date}' AND '{end_date}'
    """
    df = run_select_query(spark, query)\
        .join(dim_series, on="Series_No", how="left")

    result = df.join(user_ids_df, on="User_ID", how="inner")
    return result

def query_history(spark, user_ids_df, start_date):
    # This function check activity of users before receiving MKT messages
    query = f"""
        SELECT User_ID, datediff(day, max(DateID), cast('{start_date}' as date)) as inactive_days
         FROM dbo.fact_orders o
         WHERE Creation_date < '{start_date}'
         GROUP BY User_ID
    """
    df = run_select_query(spark, query)
    result = df.join(user_ids_df, on="User_ID", how="inner") \
               .withColumn("inactive_month", concat(ceil(col("inactive_days") / 30).cast("string"), lit(" month")))
    
    return result

def run_select_query(spark, query):
    df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("driver", driver) \
            .load()
    return df

def get_order(spark):
    query = """select DateID, Series_No, User_ID, Orders, Entries, Turnover, Prize, Draw_Period
                from dbo.fact_orders_summary
            """
    df = run_select_query(spark, query)
    return df

def get_deposit(spark):
    query = """select top_up_id, user_id as User_ID, top_up_state, top_up_amount
                FROM dbo.fact_deposit
                """
    df = run_select_query(spark, query)
    return df

def extract_data(spark, operator, filters=None, segment=None):       
    exclude_query = """
            select User_ID, RG_limit,
            receive_mail, receive_message, receive_sms, user_status
            from dbo.dim_user_authentication
            where RG_limit = 1
            or receive_mail = 0
            or receive_message = 0
            or receive_sms = 0
            or user_status <> 'Normal'
            """
    to_exclude = run_select_query(spark, exclude_query)
    dim_series = run_select_query(spark, """select SeriesNo as Series_No, 
                        GameType, GameName from dbo.dim_series""")

    # Get query based on operator selected
    if operator == "KYC no eKYC (or fail)":
        query = f""" 
            select User_ID, verification_status
            from dbo.dim_user_authentication
            where verification_status <> 'Verified'
            """
    elif operator == "All users":
        query = f""" 
            select User_ID, verification_status, nationality
            from dbo.dim_user_authentication
            """
    elif operator == "No attempt deposit":
        query = f"""
            select User_ID, attempt_depo
            from dbo.dim_user_authentication
            where attempt_depo is null
            """
    elif operator == "No success deposit":
        query = f"""
            select User_ID, attempt_depo, FTD
            from dbo.dim_user_authentication
            where FTD is null
            """
    elif operator == "No order":
        query = f"""
            select User_ID, verification_status, attempt_depo, FTD, FTP
            from dbo.dim_user_authentication
            where FTP is null
            """       
    
    # Run query based on operator selected
    if operator == "Order behavior":
        df = get_order(spark)
        
        df = df.join(to_exclude, on="User_ID", how="left_anti")\
            .join(dim_series, on="Series_No", how="left")\
            .groupBy("User_ID")\
            .agg(min("DateID").alias("First_order_date"),
                 max("DateID").alias("Last_order_date"),
                 datediff(current_date(), max("DateID")).alias("inactive_days"),
                 count_distinct(when(col("GameType")=="Lucky Day", col("Draw_Period"))).alias("distinct_series_bought"),
                 max(when(col("GameType")=="Lucky Day", col("Draw_Period"))).alias("Last_draw_series"),
                 collect_set("GameType").alias("Product_bought"),
                 sum("Orders").alias("Orders"),
                 sum("Entries").alias("Tickets"),
                 sum("Turnover").alias("Turnover"),
                 sum("Prize").alias("Prize"),
                 (sum("Turnover") - sum("Prize")).alias("GGR")
                 )
            
        # for column, (op, value) in filters.items():
        #     if op == ">=":
        #         df = df.filter(col(column) >= value)
        #     elif op == "<=":
        #         df = df.filter(col(column) <= value)
        #     elif op == "=":
        #         df = df.filter(col(column) == value)
        #     elif op == "<>":
        #         df = df.filter(col(column) != value)
    
    elif operator == "Top N by Draw series":
        df = get_order(spark)\
            .join(to_exclude, on="User_ID", how="left_anti")\
            .join(dim_series, on="Series_No", how="left")\
            .filter(col("GameType")==filters["by_product"])\

        if filters["draw_period"] > 0:
            df = df.filter(col("Draw_Period")==filters["draw_period"])
        
        if filters["by_product"] == "Instant":
            df = df.filter(col("GameName")==filters["instant_game"])
            df = df.groupBy("User_ID", "GameName")\
                .agg(
                    sum("Entries").alias("Ticket"),
                    sum("Turnover").alias("Turnover"),
                    sum("Prize").alias("Prize"),
                    (sum("Turnover")-sum("Prize")).alias("GGR")
                )
        else:
            df = df.groupBy("User_ID")\
                .agg(
                    count_distinct("Draw_Period").alias("distinct_series_bought"),
                    sum("Entries").alias("Ticket"),
                    sum("Turnover").alias("Turnover"),
                    sum("Prize").alias("Prize"),
                    (sum("Turnover")-sum("Prize")).alias("GGR")
                )

        df = df.orderBy(col(filters["by_field"]).desc())\
            .limit(filters["top"])


    elif operator == "RFM Segments":
        df = run_select_query(spark, "select User_ID, Segment from dbo.rfm_score")\
            .join(to_exclude, on="User_ID", how="left_anti")\
            .filter(col("Segment")==segment)


    elif operator == "Deposit behavior":
        df = get_deposit(spark)\
            .join(to_exclude, on="User_ID", how="left_anti")\
            .groupBy("User_ID").agg(
                count("top_up_id").alias("attempt_transaction"),
                sum(when(col("top_up_state")=='Success', col("top_up_amount"))).alias("success_amount")
            )
    
    elif operator == "Wallet balance":
        depo = get_deposit(spark)\
            .groupBy("User_ID").agg(
                sum(when(col("top_up_state")=='Success', col("top_up_amount"))).alias("success_amount")
            ).filter(col("success_amount")>0)
        withdraw_query = """ 
            select User_ID, withdrawal_amount 
            FROM dbo.fact_withdraw
            WHERE withdrawal_status = 'Success'"""
        withdraw = run_select_query(spark, withdraw_query)\
            .groupBy("User_ID").agg(sum("withdrawal_amount").alias("withdraw"))\
            .filter(col("withdraw")>0)
        prize_query = """
            select User_ID, Lottery, Turnover, Prize
            FROM dbo.fact_orders     
            """
        prize = run_select_query(spark, prize_query)\
            .groupBy("User_ID")\
            .agg(
                sum("Turnover").alias("Turnover"),
                sum(when(col("Lottery")=="Instant", col("Prize"))).alias("Instant_prize"),
                sum(when((col("Lottery")=="Lucky Day") & (col("Prize")<100000), col("Prize"))).alias("Draw_prize")
            )
        refund_query = """
            select User_ID, sum(Refund_amount) as refund
            from dbo.fact_refund
            where Memo = 'Success'
            group by User_ID
            """
        refund = run_select_query(spark, refund_query)
        df = depo.join(prize, on="User_ID", how="left")\
                .join(withdraw, on="User_ID", how="left")\
                .join(refund, on="User_ID", how="left")\
                .join(to_exclude, on="User_ID", how="left_anti")\
                .withColumn("balance",
                    col("success_amount")-col("Turnover")-col("refund")+col("Instant_prize")+col("Draw_prize")
                ).filter(col("balance")>0)\
                .select("User_ID", "balance")
        
    elif operator == "Users must exclude":
        df = to_exclude            
    
    else:
        df = run_select_query(spark, query)\
            .join(to_exclude, on="User_ID", how="left_anti")
    
    return df