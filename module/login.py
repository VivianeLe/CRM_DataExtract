import streamlit as st
from spark_session import * 
from query_utils import *

def run_login(spark):
    # --- Session state to manage login
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.username = ""
        st.session_state.jdbc_url = ""
        st.session_state.conn_str = ""

    def login(username, password):
        jdbc_url = get_jdbc(username, password)
        conn_str = get_conn(username, password)
        try:
            test_query = run_select_query(spark, "select top(1) * from dbo.dim_games", jdbc_url)
            return True, jdbc_url, conn_str
        except Exception as e:
            st.error("❌ Can not log in")
            st.error(e)
            return False, None, None

    # --- Login page
    if not st.session_state.logged_in:
        st.title("🔐 Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        
        if st.button("Login"):
            success, jdbc_url, conn_str = login(username, password)
            if success:
                st.session_state.logged_in = True
                st.session_state.username = username
                st.session_state.jdbc_url = jdbc_url
                st.session_state.conn_str = conn_str
                st.success("✅ Login successful")
                st.rerun()
            
            else:
                st.error("❌ Invalid username or password")