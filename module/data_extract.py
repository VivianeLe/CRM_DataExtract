import streamlit as st
from spark_session import * 
from query_utils import *

def run_data_extract(spark, jdbc_url):
    def draw_period_input():
        draw_period_input = st.text_input("Draw period (enter numbers separated by commas, e.g. 3,5,6 or leave blank for all)")
        if draw_period_input.strip():
            try:
                draw_periods = [int(x.strip()) for x in draw_period_input.split(',') if x.strip().isdigit()]
            except ValueError:
                st.error("❌ Please enter only integers separated by commas.")
                draw_periods = []
        else:
            draw_periods = []  # empty means all
        return draw_periods
    
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
                                       "Order behavior",
                                       "Players by Lottery Type",
                                       "Winners by Lottery Type",
                                       "Top N by Draw series",
                                       "Deposit behavior",
                                       "Wallet balance",
                                       "Bank-declined users"
                                       ])

    filters = {}
    # segment = None

    if operator == "RFM Segments":
        st.markdown("### 🎯 Select segments by RFM score")
        dim_segments = run_select_query(spark, "select * from dbo.dim_rfm_segment_list", jdbc_url)\
            .select("Segment").distinct().collect()
        dim_segments = [row["Segment"] for row in dim_segments]
        segment = st.selectbox("Segments: ", dim_segments)
        filters = {"segment": segment}
    
    elif operator == "Players by Lottery Type":
        switch = st.selectbox("Buy/Not buy", [
            "Buy product",
            "Not buy product",
            "Only buy product",
            "All combinations"
        ])
        
        by_product = None
        get_LD_player = None
        draw_periods = None

        if switch != "All combinations":
            title_map = {
            "Buy product": "All players who buy: ",
            "Not buy product": "All players who not buy: ",
            "Only buy product": "All players who only buy: "
            }
            title = title_map[switch]
            by_product = st.selectbox(title, [
                    "Lucky Day",
                    "Instant",
                    "Pick 3",
                    "Merchant App"
                ])

        if switch == "Buy product" and by_product == "Lucky Day":
            get_LD_player = st.checkbox("Filter by draw series", False)
            draw_periods = draw_period_input() if get_LD_player else None

        filters = {"buy_or_not": switch, 
                   "by_product": by_product, 
                   "get_LD_player": get_LD_player,
                   "draw_period": draw_periods
                   }

    elif operator == "Winners by Lottery Type":
        by_product = st.selectbox("By Game Type", [
            "Lucky Day",
            "Instant",
            "Pick 3",
            "Merchant App"
        ])
        draw_periods = draw_period_input()
        filters = {"draw_period": draw_periods,
                  "by_product": by_product}

    elif operator == "Top N by Draw series":
        by_product = st.selectbox("By Game Type", [
            "Lucky Day",
            "Instant",
            "Pick 3",
            "Merchant App"
        ])
        ticket_price = None
        if by_product == "Instant":
            ticket_price = run_select_query(spark, 
                                            "select distinct Unit_Price from dbo.dim_games where Lottery = 'Instant'",
                                             jdbc_url).collect()
            ticket_price = [row["Unit_Price"] for row in ticket_price]
            ticket_price.insert(0, "All Instant games")

            st.markdown('<div style="padding-left: 30px"><b>🎮 Ticket price (AED)</b></div>', unsafe_allow_html=True)
            ticket_price = st.selectbox("", ticket_price, key="ticket_price", label_visibility="collapsed")

        draw_periods = draw_period_input()

        top = st.number_input("Top N users", min_value=1, value=50, key="top")
        by_field = st.selectbox("Sort by", [
            "Turnover",
            "Ticket",
            "Prize"
        ])

        filters = {"draw_period": draw_periods,
                  "by_product": by_product,
                  "ticket_price": ticket_price,
                  "top": int(top),
                  "by_field": by_field}

    if st.button("🚀 Extract Data"):
        try: 
            # Send operator and other details to extract_data function (in query utils)
            data = extract_data(spark, operator, filters, jdbc_url)
            st.write("Extracting ", data.count(), " users...")
            file_name = f"{operator.replace(' ', '_')}.csv"

            # To use in local machine
            output_path = os.path.join(os.path.expanduser("~"), "Downloads", file_name)
            data.toPandas().to_csv(output_path, index=False)

            # For docker run
            # save_csv_file(data, file_name)
            
            st.success("✅ **Data successfully extracted, now you can download it.**\n \n "
                    "RG limit, opted out, suspend, close, locked, restricted accounts are already excluded."
                    )

        except Exception as e:
            st.error(f"❌ An error occurred while extracting or saving data:\n{e}")
            # st.error(f"❌ There is no result for your filter condition")
    
    st.markdown("---")
    st.write("Validate your list before running campaign: ")
    if st.button("📛 Download must-exclude users"):
        data = extract_data(spark, "Users must exclude", None, jdbc_url)
        st.write(data.count(), " users must be excluded from Marketing campaigns")

        file_name = "Must_exclude_users.csv"
        # output_path = os.path.join(os.path.expanduser("~"), "Downloads", file_name)
        # data.toPandas().to_csv(output_path, index=False)
        save_csv_file(data, file_name)
        st.success("✅ Data successfully extracted, now you can download it.")
