#!/usr/bin/env python3
"""
Export Economic Data to Google Sheets
Similar approach to the Border Angels automation project
"""

import os
import pandas as pd
from sqlalchemy import create_engine
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Google Sheets API scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


def get_sheets_client():
    """Authenticate with Google Sheets API using service account"""
    
    creds = Credentials.from_service_account_file(
        'credentials.json',
        scopes=SCOPES
    )
    return gspread.authorize(creds)


def get_database_connection():
    """Connect to PostgreSQL database"""
    
    return create_engine(os.getenv('DATABASE_URL'))


def export_dashboard_data():
    """Export main dashboard data to Google Sheets"""
    
    print("=" * 60)
    print("EXPORTING TO GOOGLE SHEETS")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Connect to database
    print("\n📊 Connecting to database...")
    engine = get_database_connection()
    
    # Query dashboard data
    print("📥 Fetching dashboard data...")
    dashboard_query = """
        WITH monthly_data AS (
            SELECT 
                DATE_TRUNC('month', cm.observation_date)::date AS month_date,
                cm.series_id,
                AVG(cm.value) AS value,
                AVG(cm.yoy_change) AS yoy_change
            FROM calculated_metrics cm
            WHERE cm.observation_date >= '2015-01-01'
            GROUP BY DATE_TRUNC('month', cm.observation_date), cm.series_id
        ),
        pivoted AS (
            SELECT 
                month_date AS observation_date,
                MAX(CASE WHEN series_id = 'GDP' THEN yoy_change END) AS gdp_growth_yoy,
                MAX(CASE WHEN series_id = 'UNRATE' THEN value END) AS unemployment_rate,
                MAX(CASE WHEN series_id = 'CPIAUCSL' THEN yoy_change END) AS inflation_yoy,
                MAX(CASE WHEN series_id = 'FEDFUNDS' THEN value END) AS fed_funds_rate,
                MAX(CASE WHEN series_id = 'UMCSENT' THEN value END) AS consumer_sentiment,
                MAX(CASE WHEN series_id = 'HOUST' THEN value END) AS housing_starts,
                MAX(CASE WHEN series_id = 'RSXFS' THEN yoy_change END) AS retail_sales_yoy,
                MAX(CASE WHEN series_id = 'INDPRO' THEN yoy_change END) AS industrial_prod_yoy,
                MAX(CASE WHEN series_id = 'T10Y2Y' THEN value END) AS yield_curve_spread,
                MAX(CASE WHEN series_id = 'ICSA' THEN value END) AS jobless_claims
            FROM monthly_data
            GROUP BY month_date
        ),
        filled AS (
            SELECT 
                observation_date,
                COALESCE(
                    gdp_growth_yoy,
                    LAG(gdp_growth_yoy, 1) OVER (ORDER BY observation_date),
                    LAG(gdp_growth_yoy, 2) OVER (ORDER BY observation_date),
                    LAG(gdp_growth_yoy, 3) OVER (ORDER BY observation_date)
                ) AS gdp_growth_yoy,
                unemployment_rate,
                inflation_yoy,
                fed_funds_rate,
                consumer_sentiment,
                housing_starts,
                retail_sales_yoy,
                industrial_prod_yoy,
                yield_curve_spread,
                jobless_claims
            FROM pivoted
        )
        SELECT * FROM filled
        WHERE unemployment_rate IS NOT NULL
        ORDER BY observation_date
    """
    dashboard_df = pd.read_sql(dashboard_query, engine)
    print(f"   ✓ Retrieved {len(dashboard_df)} rows of dashboard data")
    
    # Query current snapshot
    print("📥 Fetching current snapshot...")
    snapshot_query = "SELECT * FROM current_snapshot_view ORDER BY category, title"
    snapshot_df = pd.read_sql(snapshot_query, engine)
    print(f"   ✓ Retrieved {len(snapshot_df)} indicators")
    
    # Connect to Google Sheets
    print("\n🔗 Connecting to Google Sheets...")
    client = get_sheets_client()
    
    # Open spreadsheet by ID
    spreadsheet_id = os.getenv('GOOGLE_SHEET_ID')
    spreadsheet = client.open_by_key(spreadsheet_id)
    print(f"   ✓ Connected to: {spreadsheet.title}")
    
    # === Update Dashboard Data Sheet ===
    print("\n📤 Updating 'Dashboard Data' sheet...")
    try:
        worksheet = spreadsheet.worksheet('Dashboard Data')
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet('Dashboard Data', rows=1000, cols=20)
    
    # Convert dates to strings for Google Sheets
    dashboard_df['observation_date'] = dashboard_df['observation_date'].astype(str)
    
    # Fill NaN with empty string
    dashboard_df = dashboard_df.fillna('')
    
    # Write header and data
    header = dashboard_df.columns.tolist()
    data = dashboard_df.values.tolist()
    worksheet.update([header] + data)
    print(f"   ✓ Wrote {len(data)} rows to 'Dashboard Data'")
    
    # === Update Current Snapshot Sheet ===
    print("\n📤 Updating 'Current Snapshot' sheet...")
    try:
        snapshot_ws = spreadsheet.worksheet('Current Snapshot')
        snapshot_ws.clear()
    except gspread.WorksheetNotFound:
        snapshot_ws = spreadsheet.add_worksheet('Current Snapshot', rows=50, cols=15)
    
    # Convert dates to strings
    snapshot_df['as_of_date'] = snapshot_df['as_of_date'].astype(str)
    snapshot_df = snapshot_df.fillna('')
    
    # Write header and data
    header = snapshot_df.columns.tolist()
    data = snapshot_df.values.tolist()
    snapshot_ws.update([header] + data)
    print(f"   ✓ Wrote {len(data)} rows to 'Current Snapshot'")
    
    # === Summary ===
    print("\n" + "=" * 60)
    print("✅ EXPORT COMPLETE")
    print("=" * 60)
    print(f"📊 Dashboard Data: {len(dashboard_df)} rows")
    print(f"📊 Current Snapshot: {len(snapshot_df)} indicators")
    print(f"🔗 Spreadsheet URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    print("=" * 60)
    
    return True


def main():
    """Main entry point"""
    
    try:
        export_dashboard_data()
    except FileNotFoundError:
        print("❌ Error: credentials.json not found")
        print("   Make sure you downloaded the service account key from Google Cloud")
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    return True


if __name__ == "__main__":
    main()