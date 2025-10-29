import duckdb
import os

# Connect to the database
db_path = os.path.join('Sensource-API', 'data', 'ZooData.duckdb')
con = duckdb.connect(database=db_path, read_only=True)

# Query all gate count data
print("=== Gate Count Data ===\n")
result = con.execute("SELECT * FROM GateCount ORDER BY Date DESC, Gate").fetchall()

# Print the results
print(f"{'Date':<12} {'Gate':<20} {'Count':>8}")
print("-" * 42)

for row in result:
    date, gate, count = row
    print(f"{date!s:<12} {gate:<20} {count:>8}")

# Print summary statistics
print("\n=== Summary Statistics ===\n")
summary = con.execute("""
    SELECT 
        Gate,
        COUNT(*) as Days,
        SUM(GateCount) as TotalVisitors,
        AVG(GateCount) as AvgDaily,
        MAX(GateCount) as MaxDaily,
        MIN(GateCount) as MinDaily
    FROM GateCount 
    GROUP BY Gate
""").fetchall()

for row in summary:
    gate, days, total, avg, max_count, min_count = row
    print(f"{gate}:")
    print(f"  Days recorded: {days}")
    print(f"  Total visitors: {total:,}")
    print(f"  Average daily: {avg:,.0f}")
    print(f"  Max daily: {max_count:,}")
    print(f"  Min daily: {min_count:,}")
    print()

# Close connection
con.close()

def export_to_csv(output_file='daily_gate_data.csv'):
    """Export all data to CSV"""
    db_path = os.path.join(os.path.dirname(__file__), '.', 'data', 'ZooData.duckdb')
    con = duckdb.connect(database=db_path, read_only=True)
    
    df = con.execute("SELECT * FROM GateCount ORDER BY Date, Gate;").fetchdf()
    df.to_csv(output_file, index=False)
    print(f"Exported {len(df):,} records to {output_file}")
    
    con.close()

if __name__ == "__main__":
    export_to_csv()