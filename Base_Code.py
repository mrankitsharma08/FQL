import streamlit as st
import requests
import pandas as pd
from datetime import datetime, time, timedelta
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
    
    # 1. Authentication
    user_cookies = st.text_area("1. Paste Cookies", height=100, placeholder="Paste your Cookie header here...")

    # 2. Date Range Selection
    st.subheader("2. Timeframe")
    date_range = st.date_input(
        "Select Date Range",
        value=(datetime.now() - timedelta(days=7), datetime.now()),
        help="Select the start and end dates."
    )
    
    # Optional Time Selection
    use_time = st.toggle("Enable Time Filter (Hour:Min)", value=False)
    start_time = time(0, 0)
    end_time = time(23, 59)
    
    if use_time:
        col_t1, col_t2 = st.columns(2)
        start_time = col_t1.time_input("Start Time", value=time(0, 0))
        end_time = col_t2.time_input("End Time", value=time(23, 59))

    # 3. MIDs Only
    st.subheader("3. Merchant IDs")
    mid_input = st.text_area("Enter MIDs (one per line or comma-separated)", 
                             value="SPEELONLINE, JASYATRATRAINONLINE", height=150)
    
    run_btn = st.button("🚀 FETCH TPV DATA")

# --- HELPER FUNCTIONS ---
def fetch_api(query, headers):
    try:
        payload = {"query": query, "extrapolationFlag": False}
        res = requests.post('https://echoredux-internal.phonepe.com/apis/moses/v2/fql/extrapolation', 
                            headers=headers, json=payload, timeout=45)
        return res.json().get('rows', [])
    except Exception as e:
        return []

def format_cr(val):
    return f"₹ {float(val)/1e7:.2f} Cr"

# --- MAIN CONTENT ---
if not run_btn:
    st.info("💡 Enter your MIDs and select a date range to check TPV performance.")
else:
    if not user_cookies:
        st.error("Please provide valid cookies in the sidebar.")
    elif isinstance(date_range, tuple) and len(date_range) == 2:
        try:
            # Parse MIDs
            mids = [m.strip() for m in mid_input.replace('\n', ',').split(',') if m.strip()]
            mid_list_str = ', '.join(f"'{m}'" for m in mids)
            headers = {"Content-Type": "application/json", "Cookie": user_cookies}
            
            start_date, end_date = date_range
            
            # Generate daily queries for the range
            days = pd.date_range(start_date, end_date).tolist()
            all_rows = []
            
            with st.status("🔍 Querying Hermes for TPV data...", expanded=True) as status:
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = []
                    for d in days:
                        # Constructing query with optional time filter
                        # Note: If your API supports hourOfDay/minuteOfHour filters:
                        time_filter = ""
                        if use_time:
                            time_filter = f"AND date.hourOfDay >= {start_time.hour} AND date.hourOfDay <= {end_time.hour}"
                        
                        query = f"""
                            SELECT eventData.merchantId, sum(eventData.amount) 
                            FROM hermes 
                            WHERE eventData.merchantId IN ({mid_list_str})
                            AND date.dayOfMonth = {d.day} 
                            AND date.monthOfYear = {d.month} 
                            AND date.year = {d.year}
                            {time_filter}
                            AND eventType IN('CALLBACK_SUCCESS','REDEMPTION_V2_SUCCESS')
                            GROUP BY eventData.merchantId
                        """
                        futures.append(executor.submit(fetch_api, query, headers))
                    
                    for f in as_completed(futures):
                        all_rows.extend(f.result())
                status.update(label="✅ Data Retrieval Complete!", state="complete", expanded=False)

            if all_rows:
                results_df = pd.DataFrame(all_rows)
                # Find the sum/amount column
                val_col = [c for c in results_df.columns if 'sum' in c.lower() or 'amount' in c.lower()][0]
                results_df['TPV_Raw'] = pd.to_numeric(results_df[val_col]).div(100)
                
                # Group by MID across the whole range
                final_df = results_df.groupby('eventData.merchantId')['TPV_Raw'].sum().reset_index()
                final_df.columns = ['MID', 'Total_TPV']
                
                # Show any MIDs that had NO data as 0
                missing_mids = set(mids) - set(final_df['MID'])
                if missing_mids:
                    missing_df = pd.DataFrame({'MID': list(missing_mids), 'Total_TPV': 0})
                    final_df = pd.concat([final_df, missing_df], ignore_index=True)

                # --- UI REPORTING ---
                st.title(f"📊 TPV Report: {start_date} to {end_date}")
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Total MIDs", len(mids))
                active_mids = len(final_df[final_df['Total_TPV'] > 0])
                c2.metric("Active MIDs", active_mids)
                total_sum = final_df['Total_TPV'].sum()
                c3.metric("Total Volume", format_cr(total_sum))

                st.divider()

                # Results Table
                st.subheader("Merchant Performance")
                final_df['Formatted TPV'] = final_df['Total_TPV'].apply(format_cr)
                
                st.dataframe(
                    final_df[['MID', 'Formatted TPV']].sort_values(by='MID'),
                    use_container_width=True,
                    hide_index=True
                )
                
                st.download_button("📥 Download Report", final_df.to_csv(index=False), "tpv_report.csv")
            else:
                st.warning("No TPV data found for the selected MIDs and Date Range.")
                
        except Exception as e:
            st.error(f"Processing Error: {e}")
    else:
        st.warning("Please select a valid start and end date.")
