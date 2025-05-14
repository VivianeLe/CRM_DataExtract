import streamlit as st
import pandas as pd
import datetime
from spark_session import *
from query_utils import *
import builtins
# st.set_page_config(layout="wide")
# st.title("🔍 Check User Activity")

page = st.sidebar.radio("📄 Select Page", 
                            ["Update user status",
                             "Data extracting",
                            "Activity checking"
                            ])

spark = init_spark()
print("⚙️  Spark Master:", spark.sparkContext.master)
print("🔁 Parallelism:", spark.sparkContext.defaultParallelism)

if page == "Activity checking":
    markdown()
    # Banner
    st.markdown("""
    <div class="gold-banner">
        <h2>🔍 Check User Activity</h2>
    </div>
    """, unsafe_allow_html=True)

    # --- Upload user list
    uploaded_file = st.file_uploader("📤 Upload User ID list (CSV), keep column name as User_ID", type=["csv"])

    # --- Select start and end time
    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input("Start date")
        end_date = st.date_input("End date")  

    with col2:
        start_time = st.time_input("Start time", value=datetime.time(0, 0))
        end_time = st.time_input("End time", value=datetime.time(23, 59))

    start_datetime = datetime.datetime.combine(start_date, start_time)
    end_datetime = datetime.datetime.combine(end_date, end_time)

    if st.button("🚀 Submit"):
        if not uploaded_file:
            st.warning("⚠️ Please upload a CSV file with User_ID column.")
        elif start_datetime > end_datetime:
            st.warning("⚠️ Start time must be before End time.")
        else:
            progress_bar = st.progress(0, text="Starting...")

            # Read User_ID file
            df_user = read_csv_spark(spark, uploaded_file)
            user_ids = df_user.select("User_ID").distinct()

            st.success(f"📥 Uploaded {user_ids.count()} unique User_ID(s)")
            progress_bar.progress(20, text="User list loaded ✅")        

            # Query data from Azure SQL
            result = query_data(spark, user_ids, start_datetime, end_datetime)
            inactive_day = query_history(spark, user_ids, start_datetime)
            progress_bar.progress(50, text="Data queried ✅")

            if result.count() == 0:
                st.warning("⚠️ No data found for selected user(s) and time range.")
            else:
                unique_receiver = user_ids.count()
                players = result.select("User_ID").distinct().count()
                tickets = result.groupBy().agg(sum("Entries")).collect()[0][0]
                turnover = result.groupBy().agg(sum("Turnover")).collect()[0][0]
                progress_bar.progress(80, text="Summarizing data ✅")

                by_gameType = result.groupBy("Lottery").agg(
                    sum("Entries").alias("Ticket sold"),
                    sum("Turnover").alias("Turnover")
                ).toPandas()

                by_inactive = result.join(inactive_day, on="User_ID", how="left").fillna("FTP")\
                    .groupBy("inactive_month").agg(
                        count("User_ID").alias("Player"),
                        sum("Entries").alias("Ticket sold"),
                        sum("Turnover").alias("Turnover")
                    ).orderBy(
                        col("inactive_month").asc()
                    ).toPandas()

                progress_bar.progress(100, text="Done 🎉")
                progress_bar.empty()

                # --- Show results
                st.subheader("✅ **Summary Result**")
                summary_df = pd.DataFrame({
                "Metric": ["Receiver", "Active players", "Active percentage", "Ticket sold", "Turnover"],
                "Value": [
                    f"{int(unique_receiver):,}",
                    f"{int(players):,}",
                    f"{builtins.round(players/unique_receiver*100,2)}%",
                    f"{int(tickets):,}",
                    f"{int(turnover):,}"
                    ]
                })

                st.table(summary_df)

                st.subheader("🎯 Ticket sold & Turnover by Game Type")
                st.dataframe(by_gameType, use_container_width=True)

                st.subheader("🎯 Ticket sold & Turnover by inactive months")
                st.dataframe(by_inactive, use_container_width=True)

                st.markdown("---")
                if st.button("❌ Close session"):
                    spark.stop()
                    st.cache_resource.clear()
                    st.success("🛑 Spark session closed and database connection terminated.")

elif page == "Update user status":
    markdown()

    st.markdown("""
    <div class="gold-banner">
        <h2>🛠️ Update User Status</h2>
        Always update user status before extracting data!
    </div>
    """, unsafe_allow_html=True)

    # st.subheader("🔸 Update RG Limit")
    rglimit_file = st.file_uploader("📂 Upload RG limit (CSV)", type=["csv"], key="rglimit")

    # st.subheader("🔸 Update Opted Out")
    optout_file = st.file_uploader("📂 Upload Opted out (CSV)", type=["csv"], key="optout")

    # st.subheader("🔸 Update User Status")
    status_file = st.file_uploader("📂 Upload User status (CSV)", type=["csv"], key="status")

    if st.button("🚀 Update all status"):
        if (not rglimit_file) | (not optout_file) | (not status_file):
            st.warning("⚠️ Please upload full 3 files")
        else:
            try:
                # ===================== RG LIMIT =====================
                rg_df = read_csv_spark(spark, rglimit_file)\
                    .select(col("User ID").alias("User_ID")).distinct()\
                    .withColumn("RG_limit", lit(1))

                update_sql = """
                UPDATE auth
                SET auth.RG_limit = 1
                FROM dbo.dim_user_authentication AS auth
                INNER JOIN stg.rg_limit AS rg ON auth.User_ID = rg.User_ID;
                """
                update_dim_user(rg_df, "stg.rg_limit", update_sql)
                st.success("✅ RG limit successfully updated")

                # ===================== OPTED OUT =====================
                opt_df = read_csv_spark(spark, optout_file)\
                        .withColumnRenamed("user_id", "User_ID")\
                        .withColumnRenamed("message_status", "receive_message")\
                        .withColumnRenamed("mail_status", "receive_mail")\
                        .withColumnRenamed("sms_status", "receive_sms")\
                        .drop("updated_time")

                update_sql = """
                UPDATE auth
                SET auth.receive_mail = new.receive_mail,
                    auth.receive_message = new.receive_message,
                    auth.receive_sms = new.receive_sms
                FROM dbo.dim_user_authentication AS auth
                INNER JOIN stg.opt_out AS new ON auth.User_ID = new.User_ID;
                """
                update_dim_user(opt_df, "stg.opt_out", update_sql)
                st.success("✅ Opted out successfully updated")

                # ===================== USER STATUS =====================
                status_df = read_csv_spark(spark, status_file)\
                        .select(
                            col("User ID").alias("User_ID"),
                            col("User Status").alias("user_status")
                        )

                update_sql = """
                UPDATE auth
                SET auth.user_status = new.user_status
                FROM dbo.dim_user_authentication AS auth
                INNER JOIN stg.user_status AS new ON auth.User_ID = new.User_ID;
                """
                update_dim_user(status_df, "stg.user_status", update_sql)
                st.success("✅ User status successfully updated")
            except Exception as e:
                st.error(f"❌ Update failed: {e}")

elif page == "Data extracting":
    markdown()
    # Banner
    st.markdown("""
    <div class="gold-banner">
        <h2>📤 Data Extracting for Marketing Campaigns</h2>
         Always update user status before extracting data!
    </div>
    """, unsafe_allow_html=True)

    operator = st.selectbox("Filter by: ", 
                                      ["All users",
                                       "RFM Segments",
                                       "No eKYC (or fail)", 
                                       "No attempt deposit",
                                       "No success deposit",
                                       "No order",
                                       "Order behavior",
                                       "Only Instant",
                                       "Only Lucky Day",
                                       "Top N by Draw series",
                                       "Deposit behavior",
                                       "Wallet balance"
                                       ])

    filters = {}
    
    # if operator == "Order behavior":
    #     st.markdown("### 🎯 Set filters for Order behavior")
    #     available_ops = [">=", "<=", "=", "<>"]

    #     filters = {}

    #     for field in ["Turnover", "Tickets", "Orders", "inactive_days"]:
    #         col1, col2 = st.columns([1, 2])
    #         with col1:
    #             op = st.selectbox(f"{field}", options=available_ops, key=f"{field}_op")
    #         with col2:
    #             val = st.number_input("Filter value",min_value=0, value=0, key=f"{field}_val")
    #         filters[field] = (op, val)

    segment = None

    if operator == "RFM Segments":
        st.markdown("### 🎯 Select segments by RFM score")
        dim_segments = run_select_query(spark, "select * from dbo.dim_rfm_segment_list")\
            .select("Segment").distinct().collect()
        dim_segments = [row["Segment"] for row in dim_segments]
        segment = st.selectbox("Segments: ", dim_segments)

    elif operator == "Top N by Draw series":
        by_product = st.selectbox("By Game Type", [
            "Lucky Day",
            "Instant"
        ])
        ticket_price = None
        if by_product == "Instant":
            ticket_price = run_select_query(spark, "select GameType, Unit_Price from dbo.dim_series")\
                .filter(col("GameType")=='Instant')\
                .select("Unit_Price").distinct().collect()
            ticket_price = [row["Unit_Price"] for row in ticket_price]
            ticket_price.insert(0, "All Instant games")

            st.markdown('<div style="padding-left: 30px"><b>🎮 Ticket price (AED)</b></div>', unsafe_allow_html=True)
            ticket_price = st.selectbox("", ticket_price, key="ticket_price", label_visibility="collapsed")

        draw_period_input = st.text_input("Draw period (enter numbers separated by commas, e.g. 3,5,6 or leave blank for all)",
                                           key="draw_period")
        if draw_period_input.strip():
            try:
                draw_periods = [int(x.strip()) for x in draw_period_input.split(',') if x.strip().isdigit()]
            except ValueError:
                st.error("❌ Please enter only integers separated by commas.")
                draw_periods = []
        else:
            draw_periods = []  # empty means all

        top = st.number_input("Top N users", min_value=1, value=50, key="top")
        by_field = st.selectbox("Sort by", [
            "Turnover",
            "Ticket",
            "Prize",
            "GGR"
        ])

        filters = {"draw_period": draw_periods,
                  "by_product": by_product,
                  "ticket_price": ticket_price,
                  "top": int(top),
                  "by_field": by_field}

    if st.button("🚀 Extract Data"):
        try: 
            data = extract_data(spark, operator, filters, segment)
            st.write("Extracting ", data.count(), " users...")
            file_name = f"{operator.replace(' ', '_')}.csv"
            output_path = os.path.join(os.path.expanduser("~"), "Downloads", file_name)
            csv_data = data.toPandas().to_csv(output_path, index=False)

            # save_csv_file(data, file_name)
            
            st.success("✅ **Data successfully extracted, now you can download it.**\n \n "
                    "RG limit, opted out, suspend, close, locked, restricted accounts are already excluded."
                    )

        except Exception as e:
            st.error(f"❌ An error occurred while extracting or saving data:\n{e}")
            # st.error(f"❌ There is no result for your filter condition")
    
    st.markdown("---")
    st.header("Validate your list before running campaign: ")
    if st.button("📛 Download must-exclude users"):
        data = extract_data(spark, "Users must exclude")
        st.write(data.count(), " users must be excluded from Marketing campaigns")

        file_name = "Must_exclude_users.csv"
        output_path = os.path.join(os.path.expanduser("~"), "Downloads", file_name)
        csv_data = data.toPandas().to_csv(output_path, index=False)
        # save_csv_file(data, file_name)
        st.success("✅ Data successfully extracted, now you can download it.")

    if st.button("❌ Close session"):
        spark.stop()
        st.cache_resource.clear()
        st.success("🛑 Spark session closed and database connection terminated.")