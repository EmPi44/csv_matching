# Sample Ultra-Fast Pipeline Output

This is what your results will look like when the pipeline completes.

## 📁 Directory Structure

```
data/processed/ultra_fast_pipeline/
├── ultra_chunk_corrected_transactions_part_001/
│   ├── ultra_fast_matches.parquet          # 15,234 matches
│   ├── ultra_fast_owners_unmatched.csv     # 8,456 unmatched owners
│   ├── ultra_fast_transactions_unmatched.csv # 12,789 unmatched transactions
│   ├── tier1_deterministic_matches.parquet # 8,901 exact matches
│   ├── tier2_fuzzy_matches.parquet         # 6,333 fuzzy matches
│   ├── preprocessed_owners.csv             # 65,069 cleaned owners
│   ├── preprocessed_transactions.csv       # 26,290 cleaned transactions
│   └── ultra_fast_summary.md               # Detailed summary
├── ultra_chunk_corrected_transactions_part_002/
│   ├── ultra_fast_matches.parquet          # 14,567 matches
│   └── ... (similar files)
├── ...
├── ultra_combined_matches.parquet          # ALL matches combined (120,000+)
└── ultra_combined_summary.md               # Overall summary
```

## 📊 Sample Summary Report

```markdown
# Ultra-Fast Pipeline Results Summary

**Run ID**: 20250716_021500  
**Generated**: 2025-07-16 02:15:00

## 📊 Overall Statistics

| Metric | Value |
|--------|-------|
| **Total Owners** | 65,069 |
| **Total Transactions** | 210,323 |
| **Total Matches** | 124,567 |
| **Owner Match Rate** | 91.5% |
| **Transaction Match Rate** | 59.2% |

## 🎯 Tier Performance

### Tier 1: Deterministic Matches
- **Matches Found**: 89,234
- **Match Rate**: 68.7%
- **Confidence**: 100% (exact matches)

### Tier 2: Fuzzy Matches  
- **Matches Found**: 35,333
- **Match Rate**: 22.8%
- **Average Confidence**: 87.3%

## 🔍 Sample Matches

### Tier 1 Sample (Exact Matches)
| Owner ID | Transaction ID | Match Type | Confidence |
|----------|----------------|------------|------------|
| OWN_001234 | TXN_567890 | deterministic | 100.0% |
| OWN_001235 | TXN_567891 | deterministic | 100.0% |
| OWN_001236 | TXN_567892 | deterministic | 100.0% |

### Tier 2 Sample (Fuzzy Matches)
| Owner ID | Transaction ID | Match Type | Confidence | Score |
|----------|----------------|------------|------------|-------|
| OWN_004567 | TXN_890123 | fuzzy | 95.2% | 0.952 |
| OWN_004568 | TXN_890124 | fuzzy | 88.7% | 0.887 |
| OWN_004569 | TXN_890125 | fuzzy | 92.1% | 0.921 |

## 📈 Data Quality Metrics

### Unmatched Records
- **Unmatched Owners**: 5,502 (8.5%)
- **Unmatched Transactions**: 85,756 (40.8%)

### Confidence Distribution
- **High**: 28,456 (80.5%)
- **Medium**: 6,877 (19.5%)
```

## 📄 Sample Match Data (Parquet Format)

The `ultra_fast_matches.parquet` file will contain:

| Column | Type | Description | Sample Value |
|--------|------|-------------|--------------|
| `owner_id` | string | Unique owner identifier | "OWN_001234" |
| `txn_id` | string | Unique transaction identifier | "TXN_567890" |
| `match_type` | string | Type of match | "deterministic" or "fuzzy" |
| `confidence` | float | Confidence score (0-1) | 0.952 |
| `confidence_bucket` | string | Confidence category | "High" or "Medium" |
| `match_score` | float | Detailed match score | 0.952 |

## 📈 Partial Results During Processing

**During processing, you'll see files appear as each chunk completes:**

1. **First chunk completes** → `ultra_chunk_part_001/` gets populated
2. **Second chunk completes** → `ultra_chunk_part_002/` gets populated
3. **...and so on**

**You can check partial results anytime:**
```bash
# Check how many chunks have completed
ls -la data/processed/ultra_fast_pipeline/ultra_chunk_*/ultra_fast_matches.parquet | wc -l

# View partial results from first completed chunk
python -c "
import pandas as pd
df = pd.read_parquet('data/processed/ultra_fast_pipeline/ultra_chunk_corrected_transactions_part_001/ultra_fast_matches.parquet')
print(f'Chunk 1: {len(df)} matches found')
print(df.head())
"
```

## 🎯 Key Benefits of This Output

1. **Partial Results**: See matches as each chunk completes
2. **Detailed Analysis**: Separate files for each tier
3. **Quality Metrics**: Confidence scores and match rates
4. **Easy Analysis**: Parquet format for fast data analysis
5. **Comprehensive Summary**: Markdown report with all statistics

## 📊 Expected Performance

Based on your data size:
- **Total Processing Time**: ~5-10 minutes
- **Total Matches Expected**: 120,000-150,000
- **Match Rate**: 85-95% of owners
- **File Sizes**: 
  - Combined matches: ~50-100MB
  - Individual chunks: ~5-15MB each 