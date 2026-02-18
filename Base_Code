import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- PAGE CONFIG ---
st.set_page_config(page_title="Merchant TPV Tracker", page_icon="💜", layout="wide")

# Custom Styling
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { background-color: white; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    .stButton>button { background-color: #5f259f; color: white; border-radius: 8px; width: 100%; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# --- SIDEBAR: INPUTS ---
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/7/71/PhonePe_Logo.svg/1200px-PhonePe_Logo.svg.png", width=120)
    st.title("Settings")
    
    # 1. Cookies
    user_cookies = st.text_area("1. Paste Cookies", height=100)

    # 2. Date & Time Selection
    st.subheader("2. Timeframe")
    date_choice = st.date_input("Select Date", value=datetime.now() - timedelta(days=1))
    
    use_time = st.toggle("Enable Hour/Minute Filter", value=False)
    selected_hour = None
    if use_time:
        selected_hour = st.slider("Select Hour of Day", 0, 23, 12)

    # 3. Merchant JSON
    st.subheader("3. Merchant Data")
    mid_input = st.text_area("Input MIDs & Targets (JSON)", 
                             value='[{"MID":"SPEELONLINE","Target_FTD_TPV":900000010}]', height=150)
    
    run_btn = st.button("🚀 FETCH DATA")

# --- HELPER FUNCTIONS ---
def fetch_api(query, headers):
    try:
        payload = {"query": query, "extrapolationFlag": False}
        res = requests.post('https://echoredux-internal.phonepe.com/apis/moses/v2/fql/extrapolation', 
                            headers=headers, json=payload, timeout=30)
        return res.json().get('rows', [])
    except: return []

def format_cr(val):
    return f"₹ {float(val)/1e7:.2f} Cr"

# --- MAIN CONTENT ---
if not run_btn:
    st.info("💡 Adjust the sidebar settings and click 'Fetch Data' to begin.")
else:
    if not user_cookies:
        st.error("Please provide valid cookies.")
    else:
        try:
            # Load User Inputs
            target_df = pd.read_json(mid_input)
            mid_list_str = ', '.join(f"'{m}'" for m in target_df['MID'])
            headers = {"Content-Type": "application/json", "Cookie": user_cookies}
            
            # Build Query
            time_filter = f"AND date.hourOfDay = {selected_hour}" if use_time else ""
            query = f"""
                SELECT eventData.merchantId, sum(eventData.amount) 
                FROM hermes 
                WHERE eventData.merchantId IN ({mid_list_str})
                AND date.dayOfMonth = {date_choice.day} 
                AND date.monthOfYear = {date_choice.month} 
                AND date.year = {date_choice.year}
                {time_filter}
                AND eventType IN('CALLBACK_SUCCESS','REDEMPTION_V2_SUCCESS')
                GROUP BY eventData.merchantId
            """

            with st.spinner("Talking to Hermes..."):
                rows = fetch_api(query, headers)
            
            if rows:
                # Process Results
                results_df = pd.DataFrame(rows)
                # Detect the sum column (sometimes named differently by API)
                val_col = [c for c in results_df.columns if 'sum' in c.lower() or 'amount' in c.lower()][0]
                results_df['Actual_TPV'] = pd.to_numeric(results_df[val_col]).div(100)
                results_df = results_df[['eventData.merchantId', 'Actual_TPV']].rename(columns={'eventData.merchantId': 'MID'})

                # Merge with User's Target Data
                final_df = pd.merge(target_df, results_df, on='MID', how='left').fillna(0)
            else:
                # If no data found, still show targets but with 0 TPV
                final_df = target_df.copy()
                final_df['Actual_TPV'] = 0

            # --- UI REPORTING ---
            st.title(f"📊 Report for {date_choice}")
            
            # Metrics
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Merchants", len(final_df))
            zero_count = len(final_df[final_df['Actual_TPV'] == 0])
            c2.metric("Zero TPV Count", zero_count, delta=f"{zero_count} Issues", delta_color="inverse")
            total_tpv = final_df['Actual_TPV'].sum()
            c3.metric("Total Captured TPV", format_cr(total_tpv))

            st.divider()

            # The Table
            st.subheader("Detailed Performance (Target vs Actual)")
            
            # Formatting for display
            display_df = final_df.copy()
            display_df['Target'] = display_df['Target_FTD_TPV'].apply(format_cr)
            display_df['TPV'] = display_df['Actual_TPV'].apply(format_cr)
            
            # Highlights rows with zero TPV
            def color_zero(val):
                color = 'red' if val == "₹ 0.00 Cr" else 'black'
                return f'color: {color}'

            st.dataframe(
                display_df[['MID', 'Target', 'TPV']].style.applymap(color_zero, subset=['TPV']),
                use_container_width=True,
                hide_index=True
            )

        except Exception as e:
            st.error(f"Error: {e}. Check if your JSON input format is correct.")
