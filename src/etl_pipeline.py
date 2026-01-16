#!/usr/bin/env python3
"""
FRED Economic Indicators ETL Pipeline
======================================
Author: Osvaldo Ruiz
Description: Extracts economic data from FRED API, transforms with 
             calculated metrics, loads to PostgreSQL database.

Usage:
    python3 src/etl_pipeline.py
"""

import os
import sys
import pandas as pd
import numpy as np
from fredapi import Fred
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import warnings
import time

warnings.filterwarnings('ignore')
load_dotenv()


class FredETLPipeline:
    """
    ETL Pipeline for Federal Reserve Economic Data (FRED)
    
    Extracts key economic indicators, calculates derived metrics,
    and loads to PostgreSQL for analysis and visualization.
    """
    
    def __init__(self):
        """Initialize connections and define indicators to track"""
        
        # Validate environment variables
        if not os.getenv('FRED_API_KEY'):
            raise ValueError("FRED_API_KEY not found in environment variables")
        if not os.getenv('DATABASE_URL'):
            raise ValueError("DATABASE_URL not found in environment variables")
        
        self.fred = Fred(api_key=os.getenv('FRED_API_KEY'))
        self.engine = create_engine(os.getenv('DATABASE_URL'))
        
        # Economic indicators to track with categories
        self.indicators = {
            # Output & Growth
            'GDP': {
                'title': 'Gross Domestic Product',
                'category': 'Output & Growth'
            },
            'INDPRO': {
                'title': 'Industrial Production Index',
                'category': 'Output & Growth'
            },
            
            # Labor Market
            'UNRATE': {
                'title': 'Unemployment Rate',
                'category': 'Labor Market'
            },
            'ICSA': {
                'title': 'Initial Jobless Claims',
                'category': 'Labor Market'
            },
            'PAYEMS': {
                'title': 'Total Nonfarm Payrolls',
                'category': 'Labor Market'
            },
            
            # Inflation & Prices
            'CPIAUCSL': {
                'title': 'Consumer Price Index',
                'category': 'Inflation'
            },
            'PCEPI': {
                'title': 'PCE Price Index',
                'category': 'Inflation'
            },
            
            # Interest Rates & Monetary Policy
            'FEDFUNDS': {
                'title': 'Federal Funds Rate',
                'category': 'Monetary Policy'
            },
            'T10Y2Y': {
                'title': '10Y-2Y Treasury Spread',
                'category': 'Monetary Policy'
            },
            'DGS10': {
                'title': '10-Year Treasury Rate',
                'category': 'Monetary Policy'
            },
            
            # Consumer & Housing
            'UMCSENT': {
                'title': 'Consumer Sentiment Index',
                'category': 'Consumer'
            },
            'RSXFS': {
                'title': 'Retail Sales',
                'category': 'Consumer'
            },
            'HOUST': {
                'title': 'Housing Starts',
                'category': 'Housing'
            },
        }
        
        print("=" * 60)
        print("FRED ETL PIPELINE INITIALIZED")
        print("=" * 60)
        print(f"Tracking {len(self.indicators)} economic indicators")
        print(f"Database: {os.getenv('DATABASE_URL').split('@')[1]}")
        print("=" * 60)
    
    def extract_series(self, series_id: str, start_date: str = '2000-01-01') -> tuple:
        """
        Extract single series from FRED API
        
        Args:
            series_id: FRED series identifier
            start_date: Start date for historical data
            
        Returns:
            Tuple of (DataFrame, series_info dict)
        """
        try:
            # Get series data
            data = self.fred.get_series(
                series_id, 
                observation_start=start_date
            )
            
            # Get series metadata
            info = self.fred.get_series_info(series_id)
            
            # Create DataFrame
            df = pd.DataFrame({
                'observation_date': data.index,
                'value': data.values,
                'series_id': series_id
            })
            
            # Clean data
            df['observation_date'] = pd.to_datetime(df['observation_date'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.dropna(subset=['value'])
            
            print(f"  ✓ Extracted {len(df)} observations")
            return df, info
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            return None, None
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations to calculate derived metrics
        
        Calculations:
        - Month-over-month percent change
        - Year-over-year percent change
        - 3-month and 12-month rolling averages
        - Z-score (standard deviations from historical mean)
        - Percentile rank (historical context)
        """
        if df is None or df.empty:
            return None
        
        df = df.sort_values('observation_date').copy()
        
        # Month-over-month percent change
        df['mom_change'] = df['value'].pct_change() * 100
        
        # Year-over-year percent change
        # Handle different frequencies (monthly vs quarterly)
        if len(df) > 12:
            df['yoy_change'] = df['value'].pct_change(periods=12) * 100
        else:
            df['yoy_change'] = df['value'].pct_change(periods=4) * 100
        
        # Rolling averages
        df['rolling_avg_3m'] = df['value'].rolling(window=3, min_periods=1).mean()
        df['rolling_avg_12m'] = df['value'].rolling(window=12, min_periods=1).mean()
        
        # Z-score (how unusual is current value vs history)
        mean_val = df['value'].mean()
        std_val = df['value'].std()
        if std_val > 0:
            df['z_score'] = (df['value'] - mean_val) / std_val
        else:
            df['z_score'] = 0
        
        # Percentile rank
        df['percentile_rank'] = df['value'].rank(pct=True) * 100
        
        # IMPORTANT: Replace infinite values with None (NULL in database)
        import numpy as np
        df = df.replace([np.inf, -np.inf], np.nan)
        
        print(f"  ✓ Calculated 6 derived metrics")
        return df
    
    def load_metadata(self, series_id: str, info: dict, category: str):
        """Load series metadata to database"""
        
        with self.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO series_metadata 
                    (series_id, title, frequency, units, seasonal_adjustment, category, last_updated)
                VALUES 
                    (:series_id, :title, :frequency, :units, :seasonal_adj, :category, :last_updated)
                ON CONFLICT (series_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    frequency = EXCLUDED.frequency,
                    units = EXCLUDED.units,
                    seasonal_adjustment = EXCLUDED.seasonal_adjustment,
                    category = EXCLUDED.category,
                    last_updated = EXCLUDED.last_updated
            """), {
                'series_id': series_id,
                'title': self.indicators[series_id]['title'],
                'frequency': info.get('frequency_short', 'M'),
                'units': info.get('units', ''),
                'seasonal_adj': info.get('seasonal_adjustment_short', ''),
                'category': category,
                'last_updated': datetime.now()
            })
            conn.commit()
    
    def load_observations(self, df: pd.DataFrame):
        """Load raw observations to database"""
        
        observations_df = df[['series_id', 'observation_date', 'value']].copy()
        
        # Use upsert logic
        with self.engine.connect() as conn:
            for _, row in observations_df.iterrows():
                conn.execute(text("""
                    INSERT INTO observations (series_id, observation_date, value)
                    VALUES (:series_id, :observation_date, :value)
                    ON CONFLICT (series_id, observation_date) DO UPDATE SET
                        value = EXCLUDED.value
                """), {
                    'series_id': row['series_id'],
                    'observation_date': row['observation_date'],
                    'value': row['value']
                })
            conn.commit()
    
    def load_metrics(self, df: pd.DataFrame):
        """Load calculated metrics to database"""
        
        metrics_cols = ['series_id', 'observation_date', 'value', 'mom_change', 
                        'yoy_change', 'rolling_avg_3m', 'rolling_avg_12m', 
                        'z_score', 'percentile_rank']
        
        metrics_df = df[metrics_cols].copy()
        
        # Use upsert logic
        with self.engine.connect() as conn:
            for _, row in metrics_df.iterrows():
                conn.execute(text("""
                    INSERT INTO calculated_metrics 
                        (series_id, observation_date, value, mom_change, yoy_change,
                         rolling_avg_3m, rolling_avg_12m, z_score, percentile_rank)
                    VALUES 
                        (:series_id, :observation_date, :value, :mom_change, :yoy_change,
                         :rolling_avg_3m, :rolling_avg_12m, :z_score, :percentile_rank)
                    ON CONFLICT (series_id, observation_date) DO UPDATE SET
                        value = EXCLUDED.value,
                        mom_change = EXCLUDED.mom_change,
                        yoy_change = EXCLUDED.yoy_change,
                        rolling_avg_3m = EXCLUDED.rolling_avg_3m,
                        rolling_avg_12m = EXCLUDED.rolling_avg_12m,
                        z_score = EXCLUDED.z_score,
                        percentile_rank = EXCLUDED.percentile_rank
                """), row.to_dict())
            conn.commit()
    
    def process_series(self, series_id: str):
        """Process a single series through full ETL"""
        
        indicator_info = self.indicators[series_id]
        print(f"\n📊 {indicator_info['title']} ({series_id})")
        print("-" * 40)
        
        # Extract
        df, info = self.extract_series(series_id)
        if df is None:
            return False
        
        # Transform
        df_transformed = self.transform_data(df)
        if df_transformed is None:
            return False
        
        # Load
        print("  Loading to database...")
        self.load_metadata(series_id, info, indicator_info['category'])
        self.load_observations(df_transformed)
        self.load_metrics(df_transformed)
        print(f"  ✓ Loaded {len(df_transformed)} records")
        
        return True
    
    def run_full_pipeline(self):
        """Execute complete ETL for all indicators"""
        
        start_time = datetime.now()
        
        print("\n" + "=" * 60)
        print("STARTING FULL ETL PIPELINE")
        print(f"Timestamp: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        success_count = 0
        fail_count = 0
        
        for series_id in self.indicators.keys():
            try:
                if self.process_series(series_id):
                    success_count += 1
                else:
                    fail_count += 1
                    
                # Rate limiting - be nice to the API
                time.sleep(0.5)
                
            except Exception as e:
                print(f"  ✗ Failed: {e}")
                fail_count += 1
        
        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        
        print("\n" + "=" * 60)
        print("ETL PIPELINE COMPLETE")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/{len(self.indicators)}")
        print(f"✗ Failed: {fail_count}/{len(self.indicators)}")
        print(f"⏱ Duration: {duration} seconds")
        print("=" * 60)
        
        return success_count, fail_count
    
    def export_for_dashboard(self, output_path: str = 'data/dashboard_export.csv'):
        """Export pivoted data for Tableau/Google Sheets"""
        
        print(f"\n📤 Exporting data for dashboard...")
        
        query = """
        SELECT * FROM economic_dashboard_view
        WHERE observation_date >= '2010-01-01'
        ORDER BY observation_date
        """
        
        df = pd.read_sql(query, self.engine)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        
        print(f"✓ Exported {len(df)} rows to {output_path}")
        return df
    
    def export_current_snapshot(self, output_path: str = 'data/current_snapshot.csv'):
        """Export current values for KPI cards"""
        
        query = "SELECT * FROM current_snapshot_view ORDER BY category, title"
        df = pd.read_sql(query, self.engine)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        
        print(f"✓ Exported snapshot to {output_path}")
        return df


def main():
    """Main entry point"""
    
    try:
        # Initialize and run pipeline
        pipeline = FredETLPipeline()
        pipeline.run_full_pipeline()
        
        # Export data for visualization
        pipeline.export_for_dashboard()
        pipeline.export_current_snapshot()
        
        print("\n🎉 Pipeline execution successful!")
        print("Next steps:")
        print("  1. Open Tableau and connect to PostgreSQL")
        print("  2. Or import CSV files to Google Sheets")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
    