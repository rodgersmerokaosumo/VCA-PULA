"""
VCA-PULA Data Quality Check System
Processes survey data question by question with integrated SQL queries
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import json
import re

class VCA_DQC:
    def __init__(self, csv_path, sql_file='dqc_queries.sql'):
        """Initialize DQC system with CSV file path and SQL queries file"""
        self.csv_path = csv_path
        self.sql_file = sql_file
        self.data = None
        self.failed_checks = []
        self.sql_queries = {}
        self.load_data()
        self.load_sql_queries()
    
    def load_data(self):
        """Load CSV data into pandas DataFrame"""
        try:
            self.data = pd.read_csv(self.csv_path)
            print(f"[OK] Data loaded: {len(self.data)} records found")
            
            # Parse JSON responses column if it exists
            if 'responses' in self.data.columns:
                print("[INFO] Parsing JSON responses column...")
                
                def safe_json_parse(x):
                    if pd.isna(x):
                        return {}
                    try:
                        if isinstance(x, str):
                            return json.loads(x)
                        return x
                    except:
                        return {}
                
                # Parse the responses column
                self.data['responses_parsed'] = self.data['responses'].apply(safe_json_parse)
                
                # Extract common fields to separate columns for easier access
                if len(self.data) > 0 and not self.data['responses_parsed'].empty:
                    first_response = self.data['responses_parsed'].iloc[0]
                    if isinstance(first_response, dict):
                        sample_keys = list(first_response.keys())[:5]
                        print(f"[INFO] Sample response keys: {sample_keys}")
            
            # Display column names for reference
            print(f"Columns available: {list(self.data.columns)[:10]}...")
        except Exception as e:
            print(f"[ERROR] Error loading data: {e}")
            raise
    
    def load_sql_queries(self):
        """Load SQL queries from file"""
        try:
            if os.path.exists(self.sql_file):
                with open(self.sql_file, 'r') as f:
                    content = f.read()
                
                # Parse queries with markers
                query_pattern = r'-- QUERY_(\w+)_START\n(.*?)\n-- QUERY_\w+_END'
                matches = re.findall(query_pattern, content, re.DOTALL)
                
                for query_id, query_text in matches:
                    self.sql_queries[query_id] = query_text.strip()
                
                print(f"[OK] Loaded {len(self.sql_queries)} SQL queries")
                for qid in self.sql_queries:
                    print(f"    - Query {qid} loaded")
            else:
                print(f"[WARNING] SQL file {self.sql_file} not found")
        except Exception as e:
            print(f"[ERROR] Error loading SQL queries: {e}")
    
    def execute_sql_query(self, query_name):
        """
        Execute a SQL-like query on the pandas DataFrame
        Simulates SQL operations using pandas
        """
        if query_name not in self.sql_queries:
            print(f"[ERROR] Query {query_name} not found")
            return None
        
        query = self.sql_queries[query_name]
        print(f"\n[SQL] Executing Query {query_name}:")
        # Format query for display
        query_lines = query.split('\n')
        for line in query_lines[:5]:  # Show first 5 lines
            print(f"       {line}")
        if len(query_lines) > 5:
            print(f"       ...")
        
        # For Q1: Extract vca_type data
        if query_name == 'Q1':
            # Simulate the SQL query using pandas
            result_df = pd.DataFrame({
                'row_number': range(1, len(self.data) + 1),
                'response_id': self.data['response_id'],
                'vca_type': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_type_of_vca') if isinstance(x, dict) else None
                )
            })
            
            # Filter out completely null values if needed
            # But keep empty strings to catch them as errors
            result_df = result_df[
                result_df['vca_type'].notna() | 
                (result_df['vca_type'] == '')
            ].copy()
            
            print(f"[SQL] Query returned {len(result_df)} rows")
            return result_df
        
        return None
    
    def dqc_q1_vca_type(self):
        """
        DQC for Question 1: What is the type of the VCA?
        Valid values: Individual, Registered Company, Cooperative
        """
        print("\n" + "="*60)
        print("DQC CHECK: Q1 - VCA Type")
        print("="*60)
        
        # Define valid values
        valid_types = ['Individual', 'Registered Company', 'Cooperative']
        
        # Execute SQL query to get data for Q1
        df_check = self.execute_sql_query('Q1')
        
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        # Add Excel row number for reference
        df_check['row_index'] = df_check.index + 2  # +2 for Excel (header + 0-index)
        
        # Perform checks
        failed_q1 = []
        
        for idx, row in df_check.iterrows():
            error_reason = None
            
            # Check if value is missing
            if pd.isna(row['vca_type']) or row['vca_type'] == '':
                error_reason = "Missing value"
            
            # Check if value is valid (case-insensitive)
            elif str(row['vca_type']).strip() not in valid_types:
                # Try case-insensitive match
                type_lower = str(row['vca_type']).strip().lower()
                valid_lower = [v.lower() for v in valid_types]
                
                if type_lower not in valid_lower:
                    error_reason = f"Invalid value: '{row['vca_type']}'. Must be one of: {', '.join(valid_types)}"
            
            # If failed, add to failed list
            if error_reason:
                failed_q1.append({
                    'question': 'Q1',
                    'question_text': 'What is the type of the VCA?',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['vca_type'],
                    'error_reason': error_reason,
                    'valid_options': ', '.join(valid_types)
                })
        
        # Add to overall failed checks
        self.failed_checks.extend(failed_q1)
        
        # Print summary
        total_records = len(df_check)
        failed_count = len(failed_q1)
        passed_count = total_records - failed_count
        
        print(f"\n[DQC RESULTS]")
        print(f"  Total records checked: {total_records}")
        print(f"  [PASS] Passed: {passed_count} ({passed_count/total_records*100:.1f}%)")
        print(f"  [FAIL] Failed: {failed_count} ({failed_count/total_records*100:.1f}%)")
        
        if failed_count > 0:
            print(f"\nFailed records preview (first 5):")
            for fail in failed_q1[:5]:
                print(f"  Row {fail['row_number']}: {fail['error_reason']}")
        
        return failed_q1
    
    def export_failed_checks(self, output_path=None):
        """Export all failed checks to CSV"""
        if not self.failed_checks:
            print("\n[OK] No failed checks to export!")
            return
        
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"failed_dqc_checks_{timestamp}.csv"
        
        df_failed = pd.DataFrame(self.failed_checks)
        df_failed.to_csv(output_path, index=False)
        
        print(f"\n[OK] Failed checks exported to: {output_path}")
        print(f"  Total failed checks: {len(self.failed_checks)}")
        
        # Group by question for summary
        if 'question' in df_failed.columns:
            summary = df_failed.groupby('question').size()
            print("\nFailed checks by question:")
            for q, count in summary.items():
                print(f"  {q}: {count} failures")
        
        return output_path
    
    def run_all_checks(self):
        """Run all implemented DQC checks"""
        print("\n" + "="*60)
        print("RUNNING ALL DQC CHECKS")
        print("="*60)
        
        # Run Q1 check
        self.dqc_q1_vca_type()
        
        # Add more checks here as we implement them
        # self.dqc_q2_vca_position()
        # self.dqc_q3_vca_name()
        # etc...
        
        # Export failed checks
        self.export_failed_checks()


# Main execution
if __name__ == "__main__":
    # Initialize DQC system
    csv_file = "data-1755782160985.csv"
    
    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found!")
    else:
        # Create DQC instance
        dqc = VCA_DQC(csv_file)
        
        # Run specific check for Q1
        dqc.dqc_q1_vca_type()
        
        # Export failed checks
        dqc.export_failed_checks()