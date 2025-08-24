# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

VCA-PULA is a Data Quality Check (DQC) system for processing Value Chain Actor (VCA) survey data in the coffee industry supply chain. The system validates survey responses against defined business rules and generates reports on data quality issues.

## Key Commands

### Running the Main DQC System
```bash
python vca_dqc_main.py
```
This executes all data quality checks on the CSV file `data-1755782160985.csv` and generates a timestamped report of failed checks.

### Data Files
- Input: `data-1755782160985.csv` - Survey response data with JSON-encoded responses
- Output: `failed_dqc_checks_[timestamp].csv` - Report of all validation failures

## Architecture

### Core Components

1. **VCA_DQC Class** (`vca_dqc_main.py`): Main processing engine that:
   - Loads and parses CSV data with JSON responses
   - Executes SQL-like queries using pandas
   - Runs validation checks for survey questions Q1-Q12
   - Exports failed validation results

2. **SQL Query Definitions** (`dqc_queries.sql`): Contains structured queries to extract specific question responses from the JSON data structure

3. **Survey Structure** (`PROJECT_JOURNAL.md`): Documents the complete survey schema including:
   - Section 1: VCA Details (Q1-Q12) - Personal and registration information
   - Section 2: VCA Business Information (Q13-Q21) - Business categories and capacities
   - Section 3: Sourcing Questions (Q22-Q27) - Coffee sourcing details
   - Section 4: Closure Instructions (Q28-Q31) - GPS and photo capture

### Data Processing Flow

1. CSV data is loaded with JSON responses parsed into a structured format
2. SQL queries extract specific fields from the nested JSON structure
3. Each question has a dedicated validation method checking:
   - Required vs optional fields
   - Valid value constraints (e.g., age 18-99)
   - Conditional logic (e.g., Q9 required if Q8="Yes")
   - Format validation (e.g., email patterns)
4. Failed checks are aggregated and exported with row references

### Validation Rules

**Q1-Q12 Validations:**
- Q1: VCA Type - Must be: Individual/Registered Company/Cooperative
- Q2: Position - Must be: Owner/Manager
- Q3: Full Name - Required text
- Q4: Age - Numeric 18-99
- Q5: Gender - Male/Female
- Q6: Phone - Required
- Q7: Email - Optional, validated format if provided
- Q8: Has National ID - Yes/No
- Q9: National ID Number - Required if Q8=Yes
- Q10: ID Photo - Required if Q8=Yes
- Q11: Legally Registered - Yes/No
- Q12: TIN Number - Required if Q11=Yes

## Key Design Patterns

- **Modular validation**: Each question has its own `dqc_q[N]_*` method
- **SQL simulation**: Uses pandas to simulate SQL queries for data extraction
- **Conditional validation**: Handles dependencies between questions
- **Comprehensive reporting**: Tracks all failures with row numbers for Excel reference