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
    by_user_query = f""" 
        select User_ID, Lottery, sum(Entries) as Ticket_sold, 
        sum(Turnover) as Turnover, 
        sum(Prize) as Prize
        from dbo.fact_orders o 
        where Creation_date between '{start_date}' and '{end_date}'
        group by User_ID, Lottery
    """
    df = run_select_query(spark, by_user_query, jdbc_url)\
        .join(user_ids_df, on="User_ID", how='inner')

    by_user_result = df.groupBy("User_ID")\
        .agg(sum("Ticket_sold").alias("Ticket_sold"),
             sum("Turnover").alias("Turnover"),
             sum("Prize").alias("Prize"))
    
    by_lottery_result = df.groupBy("Lottery")\
        .agg(count_distinct("User_ID").alias("Player"),
            sum("Ticket_sold").alias("Ticket_sold"),
             sum("Turnover").alias("Turnover"),
             sum("Prize").alias("Prize")
        ).withColumn(
        "Avg spend", round(col("Turnover")/col("Player"),2)
        ).toPandas()
    
    by_ticket_segment = by_user_result.withColumn("ticket_segment",
            when(
                col("Ticket_sold")==1, lit("1")
            ).when(
                col("Ticket_sold")<=5, lit("<=5")
            ).when(
                col("Ticket_sold")<=10, lit("<=10")
            ).when(
                col("Ticket_sold")<=50, lit("<=50")
            ).when(
                col("Ticket_sold")<=100, lit("<=100")
            ).when(
                col("Ticket_sold")<=200, lit("<=200")
            ).otherwise(lit(">200"))
    ).groupBy("ticket_segment").agg(
        count_distinct("User_ID").alias("Player"),
        sum("Ticket_sold").alias("Ticket_sold"),
        sum("Turnover").alias("Turnover")
    ).withColumn(
        "Avg spend", round(col("Turnover")/col("Player"),2)
    ).orderBy(col("Player").desc()).toPandas()
        
    return by_user_result, by_lottery_result, by_ticket_segment

def query_history(spark, user_ids_df, start_date, jdbc_url):
    # This function check activity of users before receiving MKT messages
    # start date: date of sending MKT campaign
    # user_id_list = [str(row.User_ID) for row in user_ids_df.collect()]
    # user_ids_str = ",".join(f"'{uid}'" for uid in user_id_list)
    query = f""" 
        select User_ID, 
        concat('>= ', datediff(day, max(DateID), '{start_date}')/30, ' month') as inactive_month
        from dbo.fact_orders
        where Creation_date < '{start_date}'
        group by User_ID
    """
    result = run_select_query(spark, query, jdbc_url)\
        .join(user_ids_df, on="User_ID", how='inner')

    return result

def get_deposit(spark, jdbc_url):   
    query = """select d.user_id as User_ID, count(top_up_id) as attempt_transaction,
                sum(case when top_up_state = 'Success' then top_up_amount end) as success_amount
                FROM dbo.fact_deposit d
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM dbo.vw_abnormal_users ab
                        WHERE ab.User_ID = d.User_ID
                )
                group by d.user_id
                """
    df = run_select_query(spark, query, jdbc_url)
    return df

def extract_data(spark, operator, filters=None, jdbc_url=None): 
    # Get query based on operator selected
    if operator == "All users":    
        user_query = """ 
                select User_ID, verification_status, nationality, attempt_depo, FTD, FTP
                from dbo.dim_user_authentication u
                where registerTime is not null
                AND NOT EXISTS (
                            SELECT 1
                            FROM dbo.vw_abnormal_users ab
                            WHERE ab.User_ID = u.User_ID
                    )    
            """
        df = run_select_query(spark, user_query, jdbc_url)
    
    elif operator == "Order behavior": 
        query = """ SELECT 
                fs.User_ID,
                DATEDIFF(DAY, MAX(DateID), GETDATE()) AS inactive_days,
                SUM(Orders) AS Orders,
                SUM(Entries) AS Tickets,
                SUM(Turnover) AS Turnover,
                SUM(Prize) AS Prize
                FROM dbo.fact_orders_summary fs
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM dbo.vw_abnormal_users ab
                        WHERE ab.User_ID = fs.User_ID
                )
                and User_ID <> '1'
                GROUP BY fs.User_ID
            """
        df = run_select_query(spark, query, jdbc_url)
    
    elif operator == "Filter by Lottery Type":
        if filters["buy_or_not"] == "Buy product":
            if (filters["by_product"] == 'Lucky Day') and filters["get_LD_player"]:
                if len(filters["draw_period"]) > 0:
                    periods = ",".join(str(p) for p in filters["draw_period"])
                    query = f""" 
                    SELECT 
                        distinct User_ID
                    FROM dbo.fact_orders_summary fs
                    WHERE GameID = 72
                    AND Draw_Period in ({periods})
                    AND NOT EXISTS (
                            SELECT 1
                            FROM dbo.vw_abnormal_users ab
                            WHERE ab.User_ID = fs.User_ID
                    ) 
                    and fs.User_ID <> '1'      
                    """
                else:
                    query = f""" 
                    SELECT 
                        distinct User_ID
                    FROM dbo.fact_orders_summary fs
                    WHERE GameID = 72
                    AND NOT EXISTS (
                            SELECT 1
                            FROM dbo.vw_abnormal_users ab
                            WHERE ab.User_ID = fs.User_ID
                    )   
                    and fs.User_ID <> '1'    
                    """
            else:
                query = f""" 
                    SELECT
                    fs.User_ID,
                    g.Lottery,
                    DATEDIFF(DAY, MAX(fs.DateID), GETDATE()) AS inactive_days,
                    COUNT(DISTINCT fs.Game_series) AS distinct_series_bought,
                    MAX(fs.Draw_Period) AS Last_active_period,
                    SUM(fs.Entries) AS Tickets,
                    SUM(fs.Turnover) AS Turnover,
                    SUM(fs.Prize) AS Prize
                    FROM dbo.fact_orders_summary fs
                    JOIN dbo.dim_games g
                        ON fs.GameID = g.GameID
                    WHERE g.Lottery = '{filters["by_product"]}'
                    AND NOT EXISTS (
                            SELECT 1
                            FROM dbo.vw_abnormal_users ab
                            WHERE ab.User_ID = fs.User_ID
                    )
                    and fs.User_ID <> '1'
                    GROUP BY fs.User_ID, g.Lottery        
                    """
            df = run_select_query(spark, query, jdbc_url)
        
        else: # players not buy product
            query = """ select distinct User_ID, Lottery
                FROM dbo.fact_orders_summary fs
                JOIN dbo.dim_games g
                on fs.GameID = g.GameID
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM dbo.vw_abnormal_users ab
                    WHERE ab.User_ID = fs.User_ID
                )
            """
            df = run_select_query(spark, query, jdbc_url)
            df = df.groupBy("User_ID")\
                .agg(
                    collect_set("Lottery").alias("Product_bought")
                ).withColumn("Product_bought", concat_ws(", ", col("Product_bought")))\
                .filter(~col("Product_bought").contains(filters["by_product"]))
            
    elif operator == "Top N by Draw series":
        # Summary order data
        query = f"""select User_ID, fs.Game_series, Unit_Price, Draw_Period,
                sum(Entries) as Entries, 
                sum(Turnover) as Turnover, 
                sum(Prize) as Prize
                from dbo.fact_orders_summary fs
                join dbo.dim_games g 
                on fs.GameID = g.GameID
                WHERE Lottery = '{filters["by_product"]}'
                AND NOT EXISTS (
                        SELECT 1
                        FROM dbo.vw_abnormal_users ab
                        WHERE ab.User_ID = fs.User_ID
                )
                and fs.User_ID <> '1'
                group by User_ID, fs.Game_series, Unit_Price, Draw_Period
            """
        df = run_select_query(spark, query, jdbc_url)
        
        if filters["by_product"] == "Instant":
            if filters["ticket_price"] != 'All Instant games':
                df = df.filter(col("Unit_Price")==filters["ticket_price"])

        group_cols = ["User_ID"]
        agg_exprs = [
            sum("Entries").alias("Ticket"),
            sum("Turnover").alias("Turnover"),
            sum("Prize").alias("Prize")
        ]
        
        if (len(filters["draw_period"]) == 0) | (filters["draw_period"] == None): # all periods
            if (filters["by_product"] == "Lucky Day") | (filters["ticket_price"] == 'All Instant games'):
                agg_exprs.insert(0, count_distinct("Game_series").alias("distinct_series_bought"))
            df = df.groupBy(*group_cols).agg(*agg_exprs)\
                .orderBy(col(filters["by_field"]).desc())\
                .limit(filters["top"])

        else: # if has draw period
            df = df.filter(col("Draw_Period").isin(filters["draw_period"]))
            group_cols.insert(0, "Draw_Period")

            from pyspark.sql.window import Window

            window_spec = Window.partitionBy("Draw_Period").orderBy(col(filters["by_field"]).desc())
            df = df.groupBy(*group_cols).agg(*agg_exprs)
            df = df.withColumn("Rank", rank().over(window_spec))\
                .filter(col("Rank") <= filters["top"])\
                .withColumn("Lottery", lit(filters["by_product"]))\

        if filters["by_product"] == "Instant":
            df = df.withColumn("Unit_Price", lit(filters["ticket_price"]))


    elif operator == "RFM Segments":  
        query = f""" 
            select User_ID, Segment
            from dbo.rfm_score r
            where Segment = '{filters["segment"]}'
            and NOT EXISTS (
                            SELECT 1
                            FROM dbo.vw_abnormal_users ab
                            WHERE ab.User_ID = r.User_ID
                    )
        """   
        df = run_select_query(spark, query, jdbc_url)

    elif operator == "Deposit behavior":
        df = get_deposit(spark, jdbc_url)
            
    elif operator == "Wallet balance":        
        depo = get_deposit(spark, jdbc_url).filter(col("success_amount")>0)
        query = """select fs.User_ID, sum(Turnover) as Turnover,
                sum(case when GameID = 72 and Prize <100000 then Prize end) as Draw_prize,
                sum(case when GameID <> 72 then Prize end) as Other_prize
                from dbo.fact_orders_summary fs
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM dbo.vw_abnormal_users ab
                        WHERE ab.User_ID = fs.User_ID
                )
                group by fs.User_ID
            """
        prize = run_select_query(spark, query, jdbc_url)
        withdraw_query = """ 
            select w.User_ID, sum(withdrawal_amount) as withdraw
            FROM dbo.fact_withdraw w
            WHERE withdrawal_status not in ('Bank reverted', 'Failure') 
            AND NOT EXISTS (
                        SELECT 1
                        FROM dbo.vw_abnormal_users ab
                        WHERE ab.User_ID = w.User_ID
                )
            group by w.User_ID
            having sum(withdrawal_amount) >0
            """
        withdraw = run_select_query(spark, withdraw_query, jdbc_url)

        # prize = orders.groupBy("User_ID")\
        #     .agg(
        #         sum("Turnover").alias("Turnover"),
        #         sum(when(col("Lottery")!="Lucky Day", col("Prize")).otherwise(0)).alias("Other_prize"),
        #         sum(when((col("Lottery")=="Lucky Day") & (col("Prize")<100000), col("Prize")).otherwise(0)).alias("Draw_prize")
        #     )
        refund_query = """
            select r.User_ID, sum(Refund_amount) as refund
            from dbo.fact_refund r
            where Memo = 'Success'
            AND NOT EXISTS (
                        SELECT 1
                        FROM dbo.vw_abnormal_users ab
                        WHERE ab.User_ID = r.User_ID
                )
            group by r.User_ID
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
                and NOT EXISTS (
                            SELECT 1
                            FROM dbo.vw_abnormal_users ab
                            WHERE ab.User_ID = d.User_ID
                    )
            """
        deposit = get_deposit(spark, jdbc_url)
        df = run_select_query(spark, query, jdbc_url)\
            .join(deposit, on="User_ID", how="left")
        
    elif operator == "Users must exclude":
        df = run_select_query(spark, "select * from dbo.vw_abnormal_users", jdbc_url)  
    
    return df