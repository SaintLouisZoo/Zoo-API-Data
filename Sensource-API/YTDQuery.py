import duckdb
import pandas as pd
from datetime import datetime
import os

def query_ytd_data():
    """Query and display YTD gate count data from DuckDB"""
    
    # Connect to the database
    db_path = os.path.join(os.path.dirname(__file__), '.', 'data', 'YTD.data.duckdb')
    con = duckdb.connect(database=db_path, read_only=True)
    
    print("=" * 80)
    print("YTD GATE COUNT DATA QUERY")
    print("=" * 80)
    
    # 1. Show table schema
    print("\n1. TABLE SCHEMA:")
    schema = con.execute("DESCRIBE GateCount;").fetchdf()
    print(schema.to_string(index=False))
    
    # 2. Show total record count
    print("\n2. TOTAL RECORDS:")
    total = con.execute("SELECT COUNT(*) as total FROM GateCount;").fetchdf()
    print(f"   {total['total'][0]:,} records")
    
    # 3. Date range
    print("\n3. DATE RANGE:")
    date_range = con.execute("""
        SELECT 
            MIN(DateTime) as first_record,
            MAX(DateTime) as last_record
        FROM GateCount;
    """).fetchdf()
    print(date_range.to_string(index=False))
    
    # 4. Summary by Gate
    print("\n4. SUMMARY BY GATE:")
    gate_summary = con.execute("""
        SELECT 
            Gate,
            COUNT(*) as records,
            SUM(Ingress) as total_ingress,
            SUM(Egress) as total_egress,
            SUM(Ingress) - SUM(Egress) as net_traffic
        FROM GateCount
        GROUP BY Gate
        ORDER BY Gate;
    """).fetchdf()
    print(gate_summary.to_string(index=False))
    
    # 5. Daily totals (most recent 10 days)
    print("\n5. DAILY TOTALS (Last 10 Days):")
    daily = con.execute("""
        SELECT 
            DATE_TRUNC('day', DateTime) as date,
            Gate,
            SUM(Ingress) as daily_ingress,
            SUM(Egress) as daily_egress
        FROM GateCount
        GROUP BY date, Gate
        ORDER BY date DESC, Gate
        LIMIT 20;
    """).fetchdf()
    print(daily.to_string(index=False))
    
    # 6. Busiest 15-minute intervals
    print("\n6. BUSIEST 15-MINUTE INTERVALS (Top 10 by Ingress):")
    busiest = con.execute("""
        SELECT 
            DateTime,
            Gate,
            Ingress,
            Egress
        FROM GateCount
        ORDER BY Ingress DESC
        LIMIT 10;
    """).fetchdf()
    print(busiest.to_string(index=False))
    
    # 7. Monthly summary
    print("\n7. MONTHLY SUMMARY:")
    monthly = con.execute("""
        SELECT 
            DATE_TRUNC('month', DateTime) as month,
            Gate,
            SUM(Ingress) as monthly_ingress,
            SUM(Egress) as monthly_egress
        FROM GateCount
        GROUP BY month, Gate
        ORDER BY month DESC, Gate;
    """).fetchdf()
    print(monthly.to_string(index=False))
    
    # 8. Hour of day analysis
    print("\n8. AVERAGE TRAFFIC BY HOUR OF DAY:")
    hourly = con.execute("""
        SELECT 
            EXTRACT(HOUR FROM DateTime) as hour,
            Gate,
            ROUND(AVG(Ingress), 1) as avg_ingress,
            ROUND(AVG(Egress), 1) as avg_egress
        FROM GateCount
        GROUP BY hour, Gate
        ORDER BY hour, Gate;
    """).fetchdf()
    print(hourly.to_string(index=False))
    
    con.close()
    print("\n" + "=" * 80)
    print("QUERY COMPLETE")
    print("=" * 80)


def custom_query(sql_query):
    """Execute a custom SQL query"""
    db_path = os.path.join(os.path.dirname(__file__), '.', 'data', 'YTD.data.duckdb')
    con = duckdb.connect(database=db_path, read_only=True)
    
    try:
        result = con.execute(sql_query).fetchdf()
        print(result.to_string(index=False))
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        con.close()


def export_to_csv(output_file='ytd_gate_data.csv'):
    """Export all data to CSV"""
    db_path = os.path.join(os.path.dirname(__file__), '.', 'data', 'YTD.data.duckdb')
    con = duckdb.connect(database=db_path, read_only=True)
    
    df = con.execute("SELECT * FROM GateCount ORDER BY DateTime, Gate;").fetchdf()
    df.to_csv(output_file, index=False)
    print(f"Exported {len(df):,} records to {output_file}")
    
    con.close()


if __name__ == "__main__":
    # Run the standard queries
    query_ytd_data()
    
    # Optionally export to CSV
    export_to_csv()
    
   