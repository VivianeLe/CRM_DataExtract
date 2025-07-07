import streamlit as st
from spark_session import *
from query_utils import *
from module.login import *

spark = init_spark()

run_login(spark)

# If login successful, show app
if st.session_state.logged_in:
    from module.activity_check import run_activity_check
    from module.update_status import run_update_user_status
    from module.data_extract import run_data_extract

    st.sidebar.success(f"👋 Welcome, {st.session_state.username}")
    page = st.sidebar.radio("📄 Select Page", 
                            ["Update user status",
                             "Data extracting",
                             "Activity checking"])

    spark = init_spark()
    jdbc_url = st.session_state.jdbc_url
    conn_str = st.session_state.conn_str

    if page == "Activity checking":
        run_activity_check(spark, jdbc_url)

    elif page == "Update user status":
        run_update_user_status(spark, conn_str, jdbc_url)
        # conn_str.close()

    elif page == "Data extracting":
        run_data_extract(spark, jdbc_url)
    
    # st.markdown("---")
    if st.sidebar.button("❌ Close session"):
        spark.stop()
        st.cache_resource.clear()
        # conn_str.close()
        st.success("🛑 Spark session closed and database connection terminated.")
    
    if st.sidebar.button("🚪 Logout"):
        st.session_state.logged_in = False
        # conn_str.close()
        spark.stop()
        st.cache_resource.clear()
        st.rerun()
