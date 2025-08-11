import streamlit as st
import pandas as pd
import datetime
from spark_session import * 
from query_utils import *
import builtins 
from module.export_pdf import *

def run_activity_check(spark, jdbc_url):
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

            # Read User_ID file (list to check)
            df_user = read_csv_spark(spark, uploaded_file)
            user_ids = df_user.select("User_ID").distinct()

            st.success(f"📥 Uploaded {user_ids.count()} unique User_ID(s)")
            progress_bar.progress(20, text="User list loaded ✅")        

            # Query data from Azure SQL
            by_user, by_Lottery, by_ticket_segment = query_data(spark, user_ids, start_datetime, end_datetime, jdbc_url)
            # get inactive day of Users in list to check
            inactive_day = query_history(spark, user_ids, start_datetime, jdbc_url)
            progress_bar.progress(50, text="Data queried ✅")

            if by_user.count() == 0:
                st.warning("⚠️ No data found for selected user(s) and time range.")
            else:
                unique_receiver = user_ids.distinct().count()
                players = by_user.select("User_ID").distinct().count()
                tickets = by_user.groupBy().agg(sum("Ticket_sold")).collect()[0][0]
                turnover = by_user.groupBy().agg(sum("Turnover")).collect()[0][0]
                progress_bar.progress(80, text="Summarizing data ✅")
                
                by_inactive = by_user.join(inactive_day, on="User_ID", how="left").fillna("FTP")\
                    .groupBy("inactive_month").agg(
                        count_distinct("User_ID").alias("Reactivated player"),
                        sum("Ticket_sold").alias("Ticket sold"),
                        sum("Turnover").alias("Turnover")
                    ).withColumn(
                        "Avg spend", round(col("Turnover")/col("Reactivated player"),2)
                    ).orderBy(
                        col("inactive_month").asc()
                    ).toPandas()

                progress_bar.progress(100, text="Done 🎉")
                progress_bar.empty()                

                # --- Show results
                st.subheader("✅ **Summary Result**")
                summary_df = pd.DataFrame({
                "Metric": ["Receiver", "Active players", "Active percentage", "Ticket sold", "Turnover", "Avg spend"],
                "Value": [
                    f"{int(unique_receiver):,}",
                    f"{int(players):,}",
                    f"{builtins.round(players/unique_receiver*100,2)}%",
                    f"{int(tickets):,}",
                    f"{int(turnover):,}",
                    f"{builtins.round(turnover/players,2)}"
                    ]
                })
                st.table(summary_df)

                st.write("**Ticket sold and Turnover statistic**")
                stat = by_user.toPandas().describe().reset_index()
                stat.loc[stat['index'] == 'mean', 'index'] = 'average'
                stat.loc[stat['index'] == '50%', 'index'] = 'median'
                stat = stat[stat['index'].isin(['average', 'min', 'median', 'max'])].reset_index(drop=True)
                stat.iloc[:, 1:] = stat.iloc[:, 1:].round(2)
                stat.iloc[:, 1:] = stat.iloc[:, 1:].applymap(lambda x: f"{x:.2f}")
                st.table(stat)
                # st.table(result.toPandas().describe())

                st.subheader("🎯 By Lottery")
                st.dataframe(by_Lottery, use_container_width=True)

                st.subheader("🎯 By ticket sold")
                st.dataframe(by_ticket_segment, use_container_width=True)

                st.subheader("🎯 Reactivation result")
                st.dataframe(by_inactive, use_container_width=True)
            
            # Create pdf file
            pdf_bytes = make_pdf(start_datetime, end_datetime, summary_df, stat, 
                     by_Lottery,
                     by_ticket_segment,
                     by_inactive)

            # ---------- download link (base64) ----------
            b64 = base64.b64encode(pdf_bytes).decode()
            href = f'<a href="data:application/pdf;base64,{b64}" download="CRM_campaign_report.pdf">📄 Download PDF</a>'
            st.markdown(href, unsafe_allow_html=True)