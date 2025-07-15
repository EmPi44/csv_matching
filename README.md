# Dubai Hills Property Matching Pipeline

A Python 3.11 data-processing pipeline that links Dubai Hills property-owner records to DLD transaction records with ≥ 98% precision + recall. The pipeline features three automated matching tiers plus a lightweight manual review UI.

## Features

### Property Matching Pipeline
- **Three-tier matching system**: Deterministic, fuzzy, and manual review
- **High precision matching**: ≥ 98% precision and recall target
- **Automated preprocessing**: Data cleaning, normalization, and composite key generation
- **Fuzzy string matching**: Advanced similarity scoring for non-exact matches
- **Manual review interface**: Streamlit-based UI for human validation
- **Comprehensive reporting**: QA reports and match statistics
- **Modular architecture**: Clean separation of concerns

### CSV Processing Backend (Legacy)
- **Memory-efficient processing**: Handles large CSV files without loading them entirely into memory
- **Column removal**: Remove irrelevant columns from CSV data
- **File splitting**: Split large files into smaller chunks based on size limits
- **RESTful API**: FastAPI-based web service with automatic documentation

## Repository Structure

```
repo-root/
│
├── data/
│   ├── raw/
│   │   ├── owners/20250716/Dubai Hills.xlsx
│   │   └── transactions/20250716/
│   │       ├── corrected_transactions_part_001.csv
│   │       └── …
│   ├── processed/          # pipeline output written here
│   └── review/             # parquet with human decisions
│
├── matching/
│   ├── __init__.py
│   ├── preprocess.py       # cleaning + normalisation helpers
│   ├── deterministic.py    # tier-1 exact join
│   ├── fuzzy.py            # tier-2 fuzzy scorer
│   ├── review_helpers.py   # load & merge human approvals
│   └── pipeline.py         # main pipeline orchestration
│
├── ui/
│   └── review_app.py       # Streamlit manual-review interface
│
├── tests/
│   └── test_preprocess.py  # pytest unit tests
│
├── Dockerfile
├── requirements.txt
└── README.md
```

## Installation

### Option 1: One-Click Installation (Recommended)

**On macOS/Linux:**
```bash
chmod +x install.sh
./install.sh
```

**On Windows:**
```bash
install.bat
```

### Option 2: Manual Setup

1. Clone or download the project files
2. Create a virtual environment:

```bash
python -m venv venv
```

3. Activate the virtual environment:

**On macOS/Linux:**
```bash
source venv/bin/activate
```

**On Windows:**
```bash
venv\Scripts\activate
```

4. Install dependencies:

```bash
pip install -r requirements.txt
```

### Activating the Virtual Environment

After installation, activate the virtual environment:

**On macOS/Linux:**
```bash
source activate_env.sh
```

**On Windows:**
```bash
activate_env.bat
```

## Quick Start

### Property Matching Pipeline

1. **Run the example pipeline**:
```bash
python example_matching.py
```

This will:
- Create sample data
- Run the complete matching pipeline
- Generate outputs in `data/processed/`
- Display match statistics

2. **Use your own data**:
```python
from matching.pipeline import run_matching_pipeline

results = run_matching_pipeline(
    owners_file="path/to/owners.csv",
    transactions_file="path/to/transactions.csv"
)
```

3. **Start the review UI** (if manual review is needed):
```bash
streamlit run ui/review_app.py
```

### CSV Processing Backend (Legacy)

```bash
python main.py
```

The server will start on `http://localhost:8000`

## Matching Pipeline Details

### Tier 1: Deterministic Matching
- **Method**: Exact join on composite keys
- **Composite Key**: `project_clean + building_clean + unit_no`
- **Validation**: Area difference ≤ 1%
- **Confidence**: 1.00 (High bucket)
- **Action**: Auto-accept

### Tier 2: Fuzzy Matching
- **Method**: Fuzzy string similarity with scoring
- **Blocking**: Same project
- **Scoring Formula**:
  ```
  building_sim = token_set_ratio(bldg_owner, bldg_txn) / 100
  unit_match   = 1.0 if unit_exact else 0.0
  area_score   = max(0, 1 - abs(area_pct_diff) / 0.02)
  score        = 0.5*building_sim + 0.3*unit_match + 0.2*area_score
  ```
- **Thresholds**:
  - High: ≥ 0.90 (auto-accept)
  - Medium: 0.85-0.90 (optional review)
  - Low: 0.75-0.85 (manual review required)
  - Reject: < 0.75

### Tier 3: Manual Review
- **Input**: Low confidence matches + multiple candidates
- **Interface**: Streamlit web application
- **Actions**: Approve/Reject/Skip
- **Output**: Human decisions merged back into pipeline

## Outputs

The pipeline generates the following outputs in `data/processed/`:

- **matches.parquet**: Final matches with confidence scores
- **owners_unmatched.csv**: Owners that couldn't be matched
- **transactions_unmatched.csv**: Transactions that couldn't be matched
- **qa_report.md**: Comprehensive quality assurance report

## Configuration

### Data Format Requirements

**Owner Records** should include:
- `project`: Property project name
- `building`: Building/tower name
- `unit_number`: Unit or apartment number
- `area`: Property area (sqm or sqft)
- `owner_name`: Owner name

**Transaction Records** should include:
- `project`: Property project name
- `building`: Building/tower name
- `unit_number`: Unit or apartment number
- `area`: Property area (sqm or sqft)
- `buyer_name`: Buyer name

### Column Mapping

If your data uses different column names, update the `column_mapping` dictionaries in:
- `matching/preprocess.py` (lines ~150 and ~200)

## Testing

Run the test suite:

```bash
pytest tests/
```

Run specific tests:

```bash
pytest tests/test_preprocess.py -v
```

## API Documentation (Legacy CSV Processing)

Once the server is running, visit:
- **Interactive API docs**: `http://localhost:8000/docs`
- **Alternative docs**: `http://localhost:8000/redoc`

## Usage Examples

### Property Matching

```python
from matching.pipeline import run_matching_pipeline

# Run complete pipeline
results = run_matching_pipeline(
    owners_file="data/raw/owners/20250716/Dubai Hills.xlsx",
    transactions_file="data/raw/transactions/20250716/transactions.csv"
)

# Access results
print(f"Total matches: {results['data_volumes']['total_matches']}")
print(f"Match rate: {results['match_rates']['owner_match_rate']:.1%}")
```

### Manual Review

1. Start the review application:
```bash
streamlit run ui/review_app.py
```

2. Load review pairs from `data/review/pairs.parquet`
3. Review each pair and make decisions
4. Save decisions to `data/review/decisions_*.parquet`

### Individual Components

```python
from matching.preprocess import preprocess_owners, preprocess_transactions
from matching.deterministic import tier1_deterministic_match
from matching.fuzzy import tier2_fuzzy_match

# Preprocess data
owners_clean = preprocess_owners(owners_df)
transactions_clean = preprocess_transactions(transactions_df)

# Run individual tiers
tier1_matches, unmatched_owners, unmatched_txns = tier1_deterministic_match(
    owners_clean, transactions_clean
)

tier2_matches, final_unmatched_owners, final_unmatched_txns = tier2_fuzzy_match(
    unmatched_owners, unmatched_txns
)
```

## Monitoring and Logging

- **Logs**: Written to `logs/matching_pipeline.log`
- **Metrics**: Pipeline statistics in QA reports
- **Progress**: Real-time logging of each pipeline step

## Performance

- **Memory efficient**: Processes large datasets in chunks
- **Scalable**: Modular design allows for parallel processing
- **Fast**: Optimized algorithms for deterministic and fuzzy matching

## Contributing

1. Follow the existing code style (snake_case for Python)
2. Add tests for new functionality
3. Update documentation for any changes
4. Ensure all tests pass before submitting

## License

This project is licensed under the MIT License - see the LICENSE file for details. 