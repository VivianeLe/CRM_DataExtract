import os 
from pyspark.sql.functions import *
import tempfile
from dotenv import load_dotenv
import streamlit as st
import pyodbc

load_dotenv()
database= os.environ.get("DATABASE")
server_name = os.environ.get("SERVER")

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

def get_jdbc(user, db_password):
    jdbc_url = (
        "jdbc:sqlserver://{}.database.windows.net:1433;"
        "database={};" 
        "user={}@{};"
        "password={};" 
        "encrypt=true;trustServerCertificate=true;" 
        "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    ).format(server_name, database, user, server_name, db_password)
    return jdbc_url

def get_conn(user, db_password):
    conn_str = (
            " Driver={ODBC Driver 17 for SQL Server};"
            "Server=tcp:sql-nlbi-prd-uaen-01.database.windows.net,1433;"
            f"Database={database};Uid={user};"
            f"Pwd={db_password};Encrypt=yes;"
            "TrustServerCertificate=no;Connection Timeout=30;"
            )
    return conn_str

def read_csv_spark(spark, uploaded_file):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(uploaded_file.getbuffer())
        tmp_path = tmp.name

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(tmp_path)

    return df

def run_query(query, conn_str):
    try:
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()
    except Exception as e:
        st.error(f"❌ Failed to run {query}:\n{e}")

def run_select_query(spark, query, jdbc_url):
    df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("driver", driver) \
            .load()
    return df

def update_dim_user(df, table, query, conn_str, jdbc_url):
    run_query(f"DELETE FROM {table} WHERE User_ID is not null", conn_str)
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("driver", driver) \
        .mode("append") \
        .save()
    run_query(query, conn_str)
    # st.success(f"✅ {table} data is updated")

def query_data(spark, user_ids_df, start_date, end_date, jdbc_url):
    # This function check activity of users after receiving marketing message
    df = get_order(spark,jdbc_url).filter(
        (col("Creation_date") >= lit(start_date)) &
        (col("Creation_date") <= lit(end_date))
    )
    result = df.join(user_ids_df, on="User_ID", how="inner")
    return result

def query_history(spark, user_ids_df, start_date, jdbc_url):
    # This function check activity of users before receiving MKT messages
    df = get_order(spark, jdbc_url).filter(col("Creation_date") < lit(start_date)) \
        .groupBy("User_ID") \
        .agg(
            datediff(to_date(lit(start_date)), max("DateID")).alias("inactive_days")
        )

    result = df.join(user_ids_df, on="User_ID", how="inner") \
        .withColumn("inactive_month", when(
            col("inactive_days")==0, lit("1 month")
            ).otherwise(concat(lit("<="), ceil(col("inactive_days") / 30).cast("string"), lit(" month")))
        )

    return result

def get_series(spark, jdbc_url):
    return run_select_query(spark, """select SeriesNo as Series_No, 
                        Lottery, GameName, Unit_Price, s.GameID, s.Game_series
                            from dbo.dim_series s join dbo.dim_games g 
                            on s.GameID = g.GameID""", jdbc_url)

def get_order(spark, jdbc_url):
    query = """select DateID, Series_No, User_ID, Lottery, Order_ID, Creation_date, 
                Entries, Turnover, Prize, Draw_Period, GameID
                from dbo.fact_orders
            """
    df = run_select_query(spark, query, jdbc_url)
    return df

def get_deposit(spark, jdbc_url):
    query = """select top_up_id, user_id as User_ID, top_up_state, top_up_amount
                FROM dbo.fact_deposit
                """
    df = run_select_query(spark, query, jdbc_url)
    return df

def extract_data(spark, operator, filters=None, segment=None, jdbc_url=None): 
    to_exclude = run_select_query(spark, "select * from dbo.vw_abnormal_users", jdbc_url)      
    user_query = """ 
            select User_ID, verification_status, nationality, attempt_depo, FTD, FTP
            from dbo.dim_user_authentication
            where registerTime is not null
        """
    users = run_select_query(spark, user_query, jdbc_url)\
        .join(to_exclude, on="User_ID", how="left_anti")

    dim_series = get_series(spark, jdbc_url)
    orders = get_order(spark, jdbc_url).join(to_exclude, on="User_ID", how="left_anti")
    deposit = get_deposit(spark, jdbc_url).join(to_exclude, on="User_ID", how="left_anti")\
        .groupBy("User_ID").agg(
                count("top_up_id").alias("attempt_transaction"),
                sum(when(col("top_up_state")=='Success', col("top_up_amount"))).alias("success_amount")
            )
    
    # Get query based on operator selected
    if operator == "All users":
        df = users    

    # no eKYC users 
    
    
    elif operator == "Order behavior":
        df = orders.groupBy("User_ID")\
            .agg(
                datediff(current_date(), max("DateID")).alias("inactive_days"),
                count("Order_ID").alias("Orders"),
                sum("Entries").alias("Tickets"),
                sum("Turnover").alias("Turnover"),
                sum("Prize").alias("Prize"),
                collect_set("Lottery").alias("Product_bought")
            ).withColumn("Product_bought", concat_ws(", ", col("Product_bought")))\
            .orderBy(col("Turnover").desc())
    
    elif operator == "Filter by Lottery Type":
        df = orders.filter(col("Lottery")==filters["by_product"])

        if filters["by_product"] == "Lucky Day":
            df = df.groupBy("User_ID", "Lottery")\
            .agg(
                datediff(current_date(), max("DateID")).alias("inactive_days"),
                count_distinct("Series_No").alias("distinct_series_bought"),
                max("Draw_Period").alias("Last_active_period"),
                sum("Entries").alias("Tickets"),
                sum("Turnover").alias("Turnover"),
                sum("Prize").alias("Prize")
            )
        
        if filters["by_product"] == "Pick 3":
            df = df.groupBy("User_ID", "Lottery")\
            .agg(
                datediff(current_date(), max("DateID")).alias("inactive_days"),
                count_distinct("Series_No").alias("distinct_series_bought"),
                max("Series_No").alias("Last_series_bought"),
                max("Draw_Period").alias("Last_active_period"),
                sum("Entries").alias("Tickets"),
                sum("Turnover").alias("Turnover"),
                sum("Prize").alias("Prize")
            )
        
        else:
            df = df.withColumn("Game_series", col("GameID")*1000000+col("Series_No"))\
            .groupBy("User_ID", "Lottery")\
            .agg(
                datediff(current_date(), max("DateID")).alias("inactive_days"),
                count_distinct("Game_series").alias("distinct_games_bought"),
                max("Draw_Period").alias("Last_active_period"),
                sum("Entries").alias("Tickets"),
                sum("Turnover").alias("Turnover"),
                sum("Prize").alias("Prize")
            )
        df = df.orderBy(col("Turnover").desc())
            
    elif operator == "Top N by Draw series":
        df = orders\
            .drop("Lottery")\
            .withColumn("Game_series", col("GameID")*1000000+col("Series_No"))\
            .join(dim_series, on="Game_series", how="left")\
            .filter(col("Lottery")==filters["by_product"])\
        
        if filters["by_product"] == "Instant":
            if filters["ticket_price"] != 'All Instant games':
                df = df.filter(col("Unit_Price")==filters["ticket_price"])

        group_cols = ["User_ID"]
        agg_exprs = [
            sum("Entries").alias("Ticket"),
            sum("Turnover").alias("Turnover"),
            sum("Prize").alias("Prize"),
            (sum("Turnover") - sum("Prize")).alias("GGR")
        ]
        
        if filters["draw_period"]:
            df = df.filter(col("Draw_Period").isin(filters["draw_period"]))

        else: # all periods
            if (filters["by_product"] == "Lucky Day") | (filters["ticket_price"] == 'All Instant games'):
                agg_exprs.insert(0, count_distinct("Game_series").alias("distinct_series_bought"))

        df = df.groupBy(*group_cols).agg(*agg_exprs)
        df = df.orderBy(col(filters["by_field"]).desc())\
            .limit(filters["top"])\
            .withColumn("Draw_period", lit(", ".join(map(str, filters["draw_period"]))))\
            .withColumn("Lottery", lit(filters["by_product"]))\
        
        if filters["by_product"] == "Instant":
            df = df.withColumn("Unit_Price", lit(filters["ticket_price"]))


    elif operator == "RFM Segments":
        df = run_select_query(spark, "select User_ID, Segment from dbo.rfm_score", jdbc_url)\
            .join(to_exclude, on="User_ID", how="left_anti")\
            .filter(col("Segment")==segment)

    elif operator == "Deposit behavior":
        df = deposit
            
    elif operator == "Wallet balance":
        depo = deposit.filter(col("success_amount")>0)
        withdraw_query = """ 
            select User_ID, withdrawal_amount 
            FROM dbo.fact_withdraw
            WHERE withdrawal_status not in ('Bank reverted', 'Failure') 
            """
        withdraw = run_select_query(spark, withdraw_query, jdbc_url)\
            .groupBy("User_ID").agg(sum("withdrawal_amount").alias("withdraw"))\
            .filter(col("withdraw")>0)

        prize = orders.groupBy("User_ID")\
            .agg(
                sum("Turnover").alias("Turnover"),
                sum(when(col("Lottery")!="Lucky Day", col("Prize")).otherwise(0)).alias("Other_prize"),
                sum(when((col("Lottery")=="Lucky Day") & (col("Prize")<100000), col("Prize")).otherwise(0)).alias("Draw_prize")
            )
        refund_query = """
            select User_ID, sum(Refund_amount) as refund
            from dbo.fact_refund
            where Memo = 'Success'
            group by User_ID
            """
        refund = run_select_query(spark, refund_query, jdbc_url)
        df = depo.join(prize, on="User_ID", how="left")\
                .join(withdraw, on="User_ID", how="left")\
                .join(refund, on="User_ID", how="left")\
                .fillna(0, subset=["Turnover", "refund", "withdraw", "Other_prize", "Draw_prize"])\
                .withColumn("balance",
                    col("success_amount") -
                    coalesce(col("Turnover"), lit(0)) -
                    coalesce(col("refund"), lit(0)) -
                    coalesce(col("withdraw"), lit(0)) +
                    coalesce(col("Other_prize"), lit(0)) +
                    coalesce(col("Draw_prize"), lit(0))
                )\
                .withColumn("balance", when(col("balance")>0, col("balance")).otherwise(lit(0)))\
                .withColumn("Withdrawable_amount", 
                            coalesce(col("Other_prize"), lit(0))+
                            coalesce(col("Draw_prize"), lit(0))-
                            coalesce(col("withdraw"), lit(0))-
                            coalesce(col("Turnover"), lit(0))
                )\
                .withColumn("Withdrawable_amount", when(col("Withdrawable_amount")>0, col("Withdrawable_amount")).otherwise(lit(0)))\
                .withColumn("balance_group",
                            when(col("balance")<=200, lit("<=200"))\
                                .when(col("balance")<=1000, lit("<=1000"))\
                                    .when(col("balance")<=5000, lit("<=5000"))\
                                        .otherwise(lit(">5000"))
                            )\
                .select("User_ID", "balance", "Withdrawable_amount", "balance_group")\
        
    elif operator == "Users must exclude":
        df = to_exclude 
    
    return df