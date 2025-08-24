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
        
        # For Q1_Q12: Extract all VCA details data
        if query_name in ['Q1_Q12', 'Q1_Q2', 'Q1', 'Q2']:
            # Simulate the SQL query using pandas
            result_df = pd.DataFrame({
                'row_number': range(1, len(self.data) + 1),
                'response_id': self.data['response_id'],
                'vca_type': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_type_of_vca') if isinstance(x, dict) else None
                ),
                'vca_position': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_vca_position') if isinstance(x, dict) else None
                ),
                'vca_full_name': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_candidate_info', {}).get('name') if isinstance(x, dict) else None
                ),
                'vca_age': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_vca_age') if isinstance(x, dict) else None
                ),
                'vca_gender': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_vca_gender') if isinstance(x, dict) else None
                ),
                'vca_phone_number': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_candidate_info', {}).get('phone') if isinstance(x, dict) else None
                ),
                'vca_email': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_vca_email_address') if isinstance(x, dict) else None
                ),
                'has_national_id': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_vca_id_number_available') if isinstance(x, dict) else None
                ),
                'national_id_number': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_vca_id_number') if isinstance(x, dict) else None
                ),
                'photo_id_card': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_photo_id_card') if isinstance(x, dict) else None
                ),
                'legally_registered': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_legally_registered') if isinstance(x, dict) else None
                ),
                'tin_number': self.data['responses_parsed'].apply(
                    lambda x: x.get('q_tin_number') if isinstance(x, dict) else None
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
        
        # Execute SQL query to get data for Q1_Q12
        df_check = self.execute_sql_query('Q1_Q12')
        
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
    
    def dqc_q2_vca_position(self):
        """
        DQC for Question 2: What is the position of the VCA representative?
        Valid values: Owner, Manager
        """
        print("\n" + "="*60)
        print("DQC CHECK: Q2 - VCA Representative Position")
        print("="*60)
        
        # Define valid values
        valid_positions = ['Owner', 'Manager']
        
        # Execute SQL query to get data for Q1_Q12
        df_check = self.execute_sql_query('Q1_Q12')
        
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        # Add Excel row number for reference
        df_check['row_index'] = df_check.index + 2  # +2 for Excel (header + 0-index)
        
        # Perform checks
        failed_q2 = []
        
        for idx, row in df_check.iterrows():
            error_reason = None
            
            # Check if value is missing
            if pd.isna(row['vca_position']) or row['vca_position'] == '':
                error_reason = "Missing value"
            
            # Check if value is valid (case-insensitive)
            elif str(row['vca_position']).strip() not in valid_positions:
                # Try case-insensitive match
                position_lower = str(row['vca_position']).strip().lower()
                valid_lower = [v.lower() for v in valid_positions]
                
                if position_lower not in valid_lower:
                    error_reason = f"Invalid value: '{row['vca_position']}'. Must be one of: {', '.join(valid_positions)}"
            
            # If failed, add to failed list
            if error_reason:
                failed_q2.append({
                    'question': 'Q2',
                    'question_text': 'What is the position of the VCA representative?',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['vca_position'],
                    'error_reason': error_reason,
                    'valid_options': ', '.join(valid_positions)
                })
        
        # Add to overall failed checks
        self.failed_checks.extend(failed_q2)
        
        # Print summary
        total_records = len(df_check)
        failed_count = len(failed_q2)
        passed_count = total_records - failed_count
        
        print(f"\n[DQC RESULTS]")
        print(f"  Total records checked: {total_records}")
        print(f"  [PASS] Passed: {passed_count} ({passed_count/total_records*100:.1f}%)")
        print(f"  [FAIL] Failed: {failed_count} ({failed_count/total_records*100:.1f}%)")
        
        if failed_count > 0:
            print(f"\nFailed records preview (first 5):")
            for fail in failed_q2[:5]:
                print(f"  Row {fail['row_number']}: {fail['error_reason']}")
        
        return failed_q2
    
    def dqc_q3_vca_name(self):
        """DQC for Question 3: VCA full name (required text)"""
        print("\n" + "="*60)
        print("DQC CHECK: Q3 - VCA Full Name")
        print("="*60)
        
        df_check = self.execute_sql_query('Q1_Q12')
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        df_check['row_index'] = df_check.index + 2
        failed_q3 = []
        
        for idx, row in df_check.iterrows():
            error_reason = None
            if pd.isna(row['vca_full_name']) or str(row['vca_full_name']).strip() == '':
                error_reason = "Missing value"
            
            if error_reason:
                failed_q3.append({
                    'question': 'Q3',
                    'question_text': 'VCA full name',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['vca_full_name'],
                    'error_reason': error_reason,
                    'valid_options': 'Required text field'
                })
        
        self.failed_checks.extend(failed_q3)
        self._print_check_summary(df_check, failed_q3, "Q3")
        return failed_q3
    
    def dqc_q4_vca_age(self):
        """DQC for Question 4: VCA age (numeric 18-99)"""
        print("\n" + "="*60)
        print("DQC CHECK: Q4 - VCA Age")
        print("="*60)
        
        df_check = self.execute_sql_query('Q1_Q12')
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        df_check['row_index'] = df_check.index + 2
        failed_q4 = []
        
        for idx, row in df_check.iterrows():
            error_reason = None
            if pd.isna(row['vca_age']) or row['vca_age'] == '':
                error_reason = "Missing value"
            else:
                try:
                    age = int(row['vca_age'])
                    if age < 18 or age > 99:
                        error_reason = f"Invalid age: {age}. Must be between 18 and 99"
                except:
                    error_reason = f"Invalid value: '{row['vca_age']}'. Must be numeric between 18-99"
            
            if error_reason:
                failed_q4.append({
                    'question': 'Q4',
                    'question_text': 'VCA age',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['vca_age'],
                    'error_reason': error_reason,
                    'valid_options': '18-99'
                })
        
        self.failed_checks.extend(failed_q4)
        self._print_check_summary(df_check, failed_q4, "Q4")
        return failed_q4
    
    def dqc_q5_vca_gender(self):
        """DQC for Question 5: VCA gender (Male/Female)"""
        print("\n" + "="*60)
        print("DQC CHECK: Q5 - VCA Gender")
        print("="*60)
        
        valid_genders = ['Male', 'Female']
        df_check = self.execute_sql_query('Q1_Q12')
        
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        df_check['row_index'] = df_check.index + 2
        failed_q5 = []
        
        for idx, row in df_check.iterrows():
            error_reason = None
            if pd.isna(row['vca_gender']) or row['vca_gender'] == '':
                error_reason = "Missing value"
            elif str(row['vca_gender']).strip() not in valid_genders:
                error_reason = f"Invalid value: '{row['vca_gender']}'. Must be one of: {', '.join(valid_genders)}"
            
            if error_reason:
                failed_q5.append({
                    'question': 'Q5',
                    'question_text': 'VCA gender',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['vca_gender'],
                    'error_reason': error_reason,
                    'valid_options': ', '.join(valid_genders)
                })
        
        self.failed_checks.extend(failed_q5)
        self._print_check_summary(df_check, failed_q5, "Q5")
        return failed_q5
    
    def dqc_q6_vca_phone(self):
        """DQC for Question 6: VCA phone number (required)"""
        print("\n" + "="*60)
        print("DQC CHECK: Q6 - VCA Phone Number")
        print("="*60)
        
        df_check = self.execute_sql_query('Q1_Q12')
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        df_check['row_index'] = df_check.index + 2
        failed_q6 = []
        
        for idx, row in df_check.iterrows():
            error_reason = None
            if pd.isna(row['vca_phone_number']) or str(row['vca_phone_number']).strip() == '':
                error_reason = "Missing value"
            
            if error_reason:
                failed_q6.append({
                    'question': 'Q6',
                    'question_text': 'VCA phone number',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['vca_phone_number'],
                    'error_reason': error_reason,
                    'valid_options': 'Required phone number'
                })
        
        self.failed_checks.extend(failed_q6)
        self._print_check_summary(df_check, failed_q6, "Q6")
        return failed_q6
    
    def dqc_q7_vca_email(self):
        """DQC for Question 7: VCA email (optional, but validate format if provided)"""
        print("\n" + "="*60)
        print("DQC CHECK: Q7 - VCA Email")
        print("="*60)
        
        df_check = self.execute_sql_query('Q1_Q12')
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        df_check['row_index'] = df_check.index + 2
        failed_q7 = []
        
        import re
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        for idx, row in df_check.iterrows():
            error_reason = None
            # Email is optional, only validate if provided
            if not pd.isna(row['vca_email']) and str(row['vca_email']).strip() != '':
                email = str(row['vca_email']).strip()
                if not re.match(email_pattern, email):
                    error_reason = f"Invalid email format: '{email}'"
            
            if error_reason:
                failed_q7.append({
                    'question': 'Q7',
                    'question_text': 'VCA email',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['vca_email'],
                    'error_reason': error_reason,
                    'valid_options': 'Valid email format (optional)'
                })
        
        self.failed_checks.extend(failed_q7)
        self._print_check_summary(df_check, failed_q7, "Q7")
        return failed_q7
    
    def dqc_q8_has_national_id(self):
        """DQC for Question 8: Does VCA have National ID (Yes/No)"""
        print("\n" + "="*60)
        print("DQC CHECK: Q8 - Has National ID")
        print("="*60)
        
        valid_values = ['Yes', 'No']
        df_check = self.execute_sql_query('Q1_Q12')
        
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        df_check['row_index'] = df_check.index + 2
        failed_q8 = []
        
        for idx, row in df_check.iterrows():
            error_reason = None
            if pd.isna(row['has_national_id']) or row['has_national_id'] == '':
                error_reason = "Missing value"
            elif str(row['has_national_id']).strip() not in valid_values:
                error_reason = f"Invalid value: '{row['has_national_id']}'. Must be Yes or No"
            
            if error_reason:
                failed_q8.append({
                    'question': 'Q8',
                    'question_text': 'Does VCA have National ID?',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['has_national_id'],
                    'error_reason': error_reason,
                    'valid_options': ', '.join(valid_values)
                })
        
        self.failed_checks.extend(failed_q8)
        self._print_check_summary(df_check, failed_q8, "Q8")
        return failed_q8
    
    def dqc_q9_national_id_number(self):
        """DQC for Question 9: National ID number (conditional on Q8=Yes)"""
        print("\n" + "="*60)
        print("DQC CHECK: Q9 - National ID Number")
        print("="*60)
        
        df_check = self.execute_sql_query('Q1_Q12')
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        df_check['row_index'] = df_check.index + 2
        failed_q9 = []
        
        for idx, row in df_check.iterrows():
            error_reason = None
            # Check conditional logic: if Q8=Yes, Q9 should have value
            if str(row['has_national_id']).strip() == 'Yes':
                if pd.isna(row['national_id_number']) or str(row['national_id_number']).strip() == '':
                    error_reason = "Missing National ID number (required when Q8=Yes)"
            
            if error_reason:
                failed_q9.append({
                    'question': 'Q9',
                    'question_text': 'National ID number',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['national_id_number'],
                    'error_reason': error_reason,
                    'valid_options': 'Required when Q8=Yes'
                })
        
        self.failed_checks.extend(failed_q9)
        self._print_check_summary(df_check, failed_q9, "Q9")
        return failed_q9
    
    def dqc_q10_photo_id_card(self):
        """DQC for Question 10: Photo of National ID (conditional on Q8=Yes)"""
        print("\n" + "="*60)
        print("DQC CHECK: Q10 - Photo of National ID")
        print("="*60)
        
        df_check = self.execute_sql_query('Q1_Q12')
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        df_check['row_index'] = df_check.index + 2
        failed_q10 = []
        
        for idx, row in df_check.iterrows():
            error_reason = None
            # Check conditional logic: if Q8=Yes, Q10 should be True
            if str(row['has_national_id']).strip() == 'Yes':
                if pd.isna(row['photo_id_card']) or row['photo_id_card'] != True:
                    error_reason = "Missing photo of National ID (required when Q8=Yes)"
            
            if error_reason:
                failed_q10.append({
                    'question': 'Q10',
                    'question_text': 'Photo of National ID',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['photo_id_card'],
                    'error_reason': error_reason,
                    'valid_options': 'Required when Q8=Yes'
                })
        
        self.failed_checks.extend(failed_q10)
        self._print_check_summary(df_check, failed_q10, "Q10")
        return failed_q10
    
    def dqc_q11_legally_registered(self):
        """DQC for Question 11: Is VCA legally registered (Yes/No)"""
        print("\n" + "="*60)
        print("DQC CHECK: Q11 - Legally Registered")
        print("="*60)
        
        valid_values = ['Yes', 'No']
        df_check = self.execute_sql_query('Q1_Q12')
        
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        df_check['row_index'] = df_check.index + 2
        failed_q11 = []
        
        for idx, row in df_check.iterrows():
            error_reason = None
            if pd.isna(row['legally_registered']) or row['legally_registered'] == '':
                error_reason = "Missing value"
            elif str(row['legally_registered']).strip() not in valid_values:
                error_reason = f"Invalid value: '{row['legally_registered']}'. Must be Yes or No"
            
            if error_reason:
                failed_q11.append({
                    'question': 'Q11',
                    'question_text': 'Is VCA legally registered?',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['legally_registered'],
                    'error_reason': error_reason,
                    'valid_options': ', '.join(valid_values)
                })
        
        self.failed_checks.extend(failed_q11)
        self._print_check_summary(df_check, failed_q11, "Q11")
        return failed_q11
    
    def dqc_q12_tin_number(self):
        """DQC for Question 12: TIN number (conditional on Q11=Yes)"""
        print("\n" + "="*60)
        print("DQC CHECK: Q12 - TIN Number")
        print("="*60)
        
        df_check = self.execute_sql_query('Q1_Q12')
        if df_check is None or df_check.empty:
            print("[ERROR] No data returned from SQL query")
            return
        
        df_check['row_index'] = df_check.index + 2
        failed_q12 = []
        
        for idx, row in df_check.iterrows():
            error_reason = None
            # Check conditional logic: if Q11=Yes, Q12 should have value
            if str(row['legally_registered']).strip() == 'Yes':
                if pd.isna(row['tin_number']) or str(row['tin_number']).strip() == '':
                    error_reason = "Missing TIN number (required when Q11=Yes)"
            
            if error_reason:
                failed_q12.append({
                    'question': 'Q12',
                    'question_text': 'TIN number',
                    'row_number': row['row_index'],
                    'response_id': row['response_id'],
                    'current_value': row['tin_number'],
                    'error_reason': error_reason,
                    'valid_options': 'Required when Q11=Yes'
                })
        
        self.failed_checks.extend(failed_q12)
        self._print_check_summary(df_check, failed_q12, "Q12")
        return failed_q12
    
    def _print_check_summary(self, df_check, failed_list, question_num):
        """Helper method to print check summary"""
        total_records = len(df_check)
        failed_count = len(failed_list)
        passed_count = total_records - failed_count
        
        print(f"\n[DQC RESULTS]")
        print(f"  Total records checked: {total_records}")
        print(f"  [PASS] Passed: {passed_count} ({passed_count/total_records*100:.1f}%)")
        print(f"  [FAIL] Failed: {failed_count} ({failed_count/total_records*100:.1f}%)")
        
        if failed_count > 0:
            print(f"\nFailed records preview (first 5):")
            for fail in failed_list[:5]:
                print(f"  Row {fail['row_number']}: {fail['error_reason']}")
    
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
        
        # Run all Q1-Q12 checks
        self.dqc_q1_vca_type()
        self.dqc_q2_vca_position()
        self.dqc_q3_vca_name()
        self.dqc_q4_vca_age()
        self.dqc_q5_vca_gender()
        self.dqc_q6_vca_phone()
        self.dqc_q7_vca_email()
        self.dqc_q8_has_national_id()
        self.dqc_q9_national_id_number()
        self.dqc_q10_photo_id_card()
        self.dqc_q11_legally_registered()
        self.dqc_q12_tin_number()
        
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
        
        # Run all DQC checks
        dqc.run_all_checks()