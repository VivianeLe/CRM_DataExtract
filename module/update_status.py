import streamlit as st
from spark_session import * 
from query_utils import *

def run_update_user_status(spark, conn_str, jdbc_url):
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
                try:
                    update_dim_user(rg_df, "stg.rg_limit", update_sql, conn_str, jdbc_url)
                    st.success("✅ RG limit successfully updated")
                except Exception as e:
                    st.error("Error while updating: ", e)

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
                try:
                    update_dim_user(opt_df, "stg.opt_out", update_sql, conn_str, jdbc_url)
                    st.success("✅ Opted out successfully updated")
                except Exception as e:
                    st.error("Error while updating: ", e)

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
                try:
                    update_dim_user(status_df, "stg.user_status", update_sql,conn_str, jdbc_url)
                    st.success("✅ User status successfully updated")
                except Exception as e:
                    st.error("Error while updating: ", e)
            except Exception as e:
                st.error(f"❌ Update failed: {e}")