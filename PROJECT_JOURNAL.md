# VCA-PULA Data Processing System - Project Journal

## Project Overview

VCA-PULA is a comprehensive data processing system for analyzing Value Chain Actor (VCA) survey data in the coffee industry supply chain. The system extracts raw survey data from a PostgreSQL database, processes it through data quality checks, and transforms it into analytical formats.

## System Architecture

### Current Active Files

#### 1. Database Configuration
- **`.env`** - Database connection credentials (not in repository for security)
  - Contains DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
  - Used by all database-connected scripts

#### 2. SQL Query Files
- **`vca_raw_extract.sql`** - PostgreSQL query for extracting raw VCA survey data
  - Extracts from `common_questionnaireresponse` table
  - Processes JSONB responses into structured columns
  - Handles both `responses` and `farmer_responses` JSON fields
  - Project-specific filter: `project_id = 19023`

#### 3. Python Processing Scripts
- **`build_vca_wide_unified_v2.py`** - Main data transformation script
  - Connects to database and executes SQL extraction
  - Transforms raw data to long format for analysis
  - Pivots to wide format (one column per question)
  - Includes comprehensive Data Quality Checks (DQC)
  - Outputs timestamped files to `data/` folder
  - Arranges columns sequentially (q1, q2, q3, etc.)

### Data Flow Architecture

```
PostgreSQL Database
        ↓
vca_raw_extract.sql → Raw Survey Data
        ↓
build_vca_wide_unified_v2.py → Processing Engine
        ↓
data/vca_wide_unified_TIMESTAMP.csv → Final Output
```

### Output Structure

#### Data Folder (`data/`)
All output files are automatically timestamped and stored here:
- **`vca_wide_unified_YYYYMMDD_HHMMSS.csv`** - Main analytical dataset
  - One row per survey response (44 rows)
  - One column per question (348+ columns)
  - Sequential question ordering (q1, q2, q3, ...)
  - Includes DQC flags for data quality assessment

## Survey Structure Documentation

### Section 1: VCA Details (Questions 1-12)

| Q# | Question | Response Type | Validation Rules |
|----|----------|---------------|------------------|
| Q1 | VCA Type | Choice | Individual/Registered Company/Cooperative |
| Q2 | VCA Position | Choice | Owner/Manager |
| Q3 | Full Name | Text | Required |
| Q4 | Age | Numeric | 18-99 years |
| Q5 | Gender | Choice | Male/Female |
| Q6 | Phone Number | Text | Required, 9-15 digits |
| Q7 | Email | Text | Optional, valid format |
| Q8 | Has National ID | Choice | Yes/No |
| Q9 | National ID Number | Text | Required if Q8=Yes |
| Q10 | ID Photo | Boolean | Required if Q8=Yes |
| Q11 | Legally Registered | Choice | Yes/No |
| Q12 | TIN Number | Text | Required if Q11=Yes |

### Section 2: VCA Business Information (Questions 13-21)

| Q# | Question | Response Type | Categories | Validation Rules |
|----|----------|---------------|------------|------------------|
| Q13 | Business Category | Multi-choice | GF, HS, WH, MILL, SHOP, STORE, TRADER, ROASTER, EXPORTER, EXTRACTOR, OTHER | Required |
| Q14 | Other Category Specification | Text | Conditional | Required if Q13=Other; Must be valid Q13 category |
| Q15 | Business Name | Text | Per category | Required for selected categories |
| Q16 | Business Address | Text | Per category | Required for selected categories |
| Q18 | Max Operating Capacity | Numeric | Per category (kg/day) | Must be positive number |
| Q19 | Max Storage Capacity | Numeric | Per category (kg/day) | Must be positive number |
| Q20 | Hullers Operated | Numeric | HS category only | Must be positive integer |
| Q21 | Processing Throughput | Choice | HS category (small/medium/large) | Required for HS category |

### Section 3: Sourcing Questions (Questions 22-27)

| Q# | Question | Response Type | Options |
|----|----------|---------------|---------|
| Q22 | Coffee Type | Multi-choice | Arabica/Robusta/Both/Does not apply |
| Q23 | Coffee Form | Multi-choice | Red Cherries/Kiboko/Parchment/DRUGAR/FAQ/Graded/Roasted |
| Q24 | Districts | Text | Source districts |
| Q25 | Annual Volume | Numeric | Kilograms per year |
| Q26 | Source Type | Multi-choice | Farmers/Trader/Cooperative/Exporter/Other |
| Q27 | Other Sources | Text | If Q26=Other |

### Section 4: Closure Instructions (Questions 28-31)

| Q# | Question | Response Type | Format |
|----|----------|---------------|--------|
| Q28 | GPS Coordinates | JSON | Latitude/Longitude |
| Q29 | Sign Post Photo | Boolean | Required |
| Q30 | Premise Photo | Boolean | Required |
| Q31 | Unique VCA ID | Auto-generated | UC/F/16digits |

## Data Quality Checks (DQC)

### Validation Categories
1. **Presence Check** - Required fields have values
2. **Choice Validation** - Values match allowed options
3. **Numeric Validation** - Numbers within valid ranges
4. **Dependency Check** - Conditional requirements met
5. **Contact Validation** - Email/phone format verification
6. **GPS Validation** - Coordinate ranges (-90≤lat≤90, -180≤lon≤180)
7. **Business Logic Validation** - Complex conditional business rules

### DQC Output Columns
For each question, the following DQC flags are generated:
- `q[N]_question__dq_present` - Has non-empty value
- `q[N]_question__dq_valid_choice` - Value in allowed choices
- `q[N]_question__dq_numeric_ok` - Numeric value in range
- `q[N]_question__dq_dependency_ok` - Conditional requirements met
- `q[N]_question__dq_contact_ok` - Contact info format valid
- `q[N]_question__dq_gps_ok` - GPS coordinates valid
- `q[N]_question__dq_pass` - Overall pass/fail
- `q[N]_question__dq_failed_reason` - Specific failure reasons

### Key Validation Rules

#### Q14: Other Business Category Validation
**Rule**: If "Other" is selected in Q13 (business categories), then Q14 must:
1. **Not be empty** (dependency check)
2. **Contain a valid Q13 category** (choice validation)

**Valid Q14 Categories**:
- Grading facilities, Hulling station, Warehouses, Wet mills/Pulperly
- Coffee shops/brewers, Stores, Traders, Roasters/Roasteries
- Exporters, Coffee extractors

**Smart Matching**: Supports common variations
- "trader" → "Traders" ✅
- "huller" → "Hulling station" ✅  
- "grader" → "Grading facilities" ✅

**Failure Reasons**:
- `missing_q14_when_other_selected` - Q14 empty when Q13=Other
- `q14_not_valid_category` - Q14 value not a recognized category

#### Other Key Validation Rules
- **Q8/Q9**: National ID dependency - Q9 required if Q8=Yes
- **Q11/Q12**: TIN dependency - Q12 required if Q11=Yes  
- **Category Dependencies**: Business details required for selected Q13 categories
- **Age Range**: Q4 must be 18-99 years
- **Contact Format**: Email and phone number format validation
- **GPS Coordinates**: Latitude (-90 to 90), Longitude (-180 to 180)

## Historical Archive

### Archive Folder
Contains previous versions and development files:
- Original `vca_dqc_main.py` - Legacy DQC system
- `dqc_queries.sql` - Old SQL query format
- Various CSV exports from development phase
- `CLAUDE.md` - Previous documentation

## Usage Instructions

### Basic Data Extraction
```bash
# Extract and process VCA survey data with DQC and category labeling
python build_vca_wide_unified_v2.py --sql vca_raw_extract.sql --include-dqc --label-categories
```

### Output Files
- Main dataset: `data/vca_wide_unified_YYYYMMDD_HHMMSS.csv`
- Contains 44 survey responses across 350+ columns (including Q14)
- Questions ordered sequentially (q1, q2, q3, ..., q14, q15, ...)
- Category-specific answers prefixed (e.g., "HS: value", "GF: value")
- Full DQC coverage including business logic validation

## Development Log

### Recent Updates (August 2024)

#### Version 2.1 - Q14 Business Logic Validation
- **Q14 Validation Rule** - Added conditional validation for Q13 "Other" selections
- **Smart Category Matching** - Fuzzy matching for common category name variations
- **Enhanced DQC Coverage** - Now validates business logic dependencies
- **SQL Integration** - Q14 field (`q_other_business_category`) extracted from database
- **Sequential Processing** - Q14 properly positioned between Q13 and Q15 in output

#### Version 2.0 - System Restructuring  
1. **Code Restructuring** - Reorganized `build_vca_wide_unified_v2.py` for better readability
2. **Column Sorting** - Implemented sequential question ordering (q1→q2→q3...)
3. **Timestamp Integration** - All outputs now include timestamps
4. **Data Folder** - Centralized output location
5. **Enhanced Documentation** - Comprehensive code comments and structure
6. **Archive Organization** - Moved legacy files to Archive folder

### System Requirements
- Python 3.8+
- pandas, sqlalchemy, psycopg2-binary
- PostgreSQL database access
- Environment variables configured in `.env`

## Future Enhancements
- Additional DQC rules for business logic validation
- Export to multiple formats (Excel, JSON)
- Automated reporting dashboard
- Integration with visualization tools