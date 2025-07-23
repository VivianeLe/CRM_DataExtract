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
    dim_games = run_select_query(spark, "select * from dbo.dim_games", jdbc_url)
    query = f""" 
        select User_ID, Entries, Turnover, Prize, GameID
        from dbo.fact_orders
        where Creation_date between '{start_date}' and '{end_date}'
    """
    result = run_select_query(spark, query, jdbc_url)\
        .join(user_ids_df, on="User_ID", how="inner")\
        .join(dim_games, on="GameID", how="left")
    return result

def query_history(spark, user_ids_df, start_date, jdbc_url):
    # This function check activity of users before receiving MKT messages
    # start date: date of sending MKT campaign
    query = f""" 
        select User_ID, max(DateID) as last_order_date
        from dbo.fact_orders
        where Creation_date < '{start_date}'
        group by User_ID
    """
    df = run_select_query(spark, query, jdbc_url)\
        .join(user_ids_df, on="User_ID", how="inner")\
        .withColumn("inactive_days", datediff(to_date(lit(start_date)), to_date(col("last_order_date"))))

    result = df.withColumn("inactive_month", when(
            col("inactive_days")==0, lit("1 month")
            ).otherwise(concat(lit("<="), ceil(col("inactive_days") / 30).cast("string"), lit(" month")))
        )

    return result

def get_order(spark, jdbc_url):
    to_exclude = run_select_query(spark, "select * from dbo.vw_abnormal_users", jdbc_url)   
    dim_games = run_select_query(spark, "select * from dbo.dim_games", jdbc_url)
    query = """select DateID, Game_series, User_ID, Orders, 
                Entries, Turnover, Prize, Draw_Period, GameID
                from dbo.fact_orders_summary
            """
    df = run_select_query(spark, query, jdbc_url).join(to_exclude, on="User_ID", how="left_anti")\
        .join(dim_games, on="GameID", how="left")
    return df

def get_deposit(spark, jdbc_url):
    to_exclude = run_select_query(spark, "select * from dbo.vw_abnormal_users", jdbc_url)   
    query = """select top_up_id, user_id as User_ID, top_up_state, top_up_amount
                FROM dbo.fact_deposit
                """
    df = run_select_query(spark, query, jdbc_url)\
        .join(to_exclude, on="User_ID", how="left_anti")\
        .groupBy("User_ID").agg(
                count("top_up_id").alias("attempt_transaction"),
                sum(when(col("top_up_state")=='Success', col("top_up_amount"))).alias("success_amount"))
    
    return df

def extract_data(spark, operator, filters=None, segment=None, jdbc_url=None): 

    # Get query based on operator selected
    if operator == "All users":
        to_exclude = run_select_query(spark, "select * from dbo.vw_abnormal_users", jdbc_url)      
        user_query = """ 
                select User_ID, verification_status, nationality, attempt_depo, FTD, FTP
                from dbo.dim_user_authentication
                where registerTime is not null
            """
        df = run_select_query(spark, user_query, jdbc_url)\
            .join(to_exclude, on="User_ID", how="left_anti") 
    
    elif operator == "Order behavior":
        orders = get_order(spark, jdbc_url)
        df = orders.groupBy("User_ID")\
            .agg(
                datediff(current_date(), max("DateID")).alias("inactive_days"),
                sum("Orders").alias("Orders"),
                sum("Entries").alias("Tickets"),
                sum("Turnover").alias("Turnover"),
                sum("Prize").alias("Prize")
            ).orderBy(col("Turnover").desc())
    
    elif operator == "Filter by Lottery Type":
        orders = get_order(spark, jdbc_url)

        if filters["buy_or_not"] == "Buy product":
            df = orders.filter(col("Lottery")==filters["by_product"])
            df = df.groupBy("User_ID", "Lottery")\
            .agg(
                datediff(current_date(), max("DateID")).alias("inactive_days"),
                count_distinct("Game_series").alias("distinct_series_bought"),
                max("Draw_Period").alias("Last_active_period"),
                sum("Entries").alias("Tickets"),
                sum("Turnover").alias("Turnover"),
                sum("Prize").alias("Prize")
            )
            df = df.orderBy(col("Turnover").desc())
        
        else: # players not buy product
            df = orders.groupBy("User_ID")\
                .agg(
                    collect_set("Lottery").alias("Product_bought")
                ).withColumn("Product_bought", concat_ws(", ", col("Product_bought")))\
                .filter(~col("Product_bought").contains(filters["by_product"]))
            
    elif operator == "Top N by Draw series":
        orders = get_order(spark, jdbc_url)
        df = orders\
            .filter(col("Lottery")==filters["by_product"])\
        
        if filters["by_product"] == "Instant":
            if filters["ticket_price"] != 'All Instant games':
                df = df.filter(col("Unit_Price")==filters["ticket_price"])

        group_cols = ["User_ID"]
        agg_exprs = [
            sum("Entries").alias("Ticket"),
            sum("Turnover").alias("Turnover"),
            sum("Prize").alias("Prize")
        ]
        
        if filters["draw_period"]:
            df = df.filter(col("Draw_Period").isin(filters["draw_period"]))
            group_cols.insert(0, "Draw_Period")

        else: # all periods
            if (filters["by_product"] == "Lucky Day") | (filters["ticket_price"] == 'All Instant games'):
                agg_exprs.insert(0, count_distinct("Game_series").alias("distinct_series_bought"))
        
        from pyspark.sql.window import Window

        window_spec = Window.partitionBy("Draw_Period").orderBy(col(filters["by_field"]).desc())

        df = df.groupBy(*group_cols).agg(*agg_exprs)
        df = df.withColumn("Rank", rank().over(window_spec))\
            .filter(col("Rank") <= filters["top"])\
            .withColumn("Lottery", lit(filters["by_product"]))\
            # .withColumn("Draw_period", lit(", ".join(map(str, filters["draw_period"]))))\
        
        if filters["by_product"] == "Instant":
            df = df.withColumn("Unit_Price", lit(filters["ticket_price"]))


    elif operator == "RFM Segments":
        to_exclude = run_select_query(spark, "select * from dbo.vw_abnormal_users", jdbc_url)      
        df = run_select_query(spark, "select User_ID, Segment from dbo.rfm_score", jdbc_url)\
            .join(to_exclude, on="User_ID", how="left_anti")\
            .filter(col("Segment")==segment)

    elif operator == "Deposit behavior":
        df = get_deposit(spark, jdbc_url)
            
    elif operator == "Wallet balance":        
        depo = get_deposit(spark, jdbc_url).filter(col("success_amount")>0)
        orders = get_order(spark, jdbc_url)
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
                .select("User_ID", "balance", "Withdrawable_amount", "balance_group")
        
    elif operator == "Bank-declined users":
        query = """ 
                select distinct User_ID
                from dbo.fact_deposit d 
                join dbo.dim_fail_deposit_group f 
                on d.gateway_memo = f.gateway_memo
                where fail_group = 'Bank declined'
            """
        deposit = get_deposit(spark, jdbc_url)
        to_exclude = run_select_query(spark, "select * from dbo.vw_abnormal_users", jdbc_url)
        df = run_select_query(spark, query, jdbc_url)\
            .join(to_exclude, on="User_ID", how="left_anti")\
            .join(deposit, on="User_ID", how="left")
        
    elif operator == "Users must exclude":
        df = run_select_query(spark, "select * from dbo.vw_abnormal_users", jdbc_url)  
    
    return df