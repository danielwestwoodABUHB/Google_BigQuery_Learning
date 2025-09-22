# Google_BigQuery_Learning
Test codes to use with Google BQ sandbox

Link: https://console.cloud.google.com/

## Repository Contents

This repository contains various Python scripts for learning and working with Google BigQuery:

- **COVID-19 Data Analysis**: Scripts for analyzing COVID-19 data using Prophet forecasting
- **Birth Weight & BMI Analysis**: Statistical analysis scripts for CDC birth data  
- **Date Table Creation**: Scripts for creating dimensional date tables
- **Test Script**: Comprehensive testing framework for validating functionality

## Getting Started

### Prerequisites

Install the required packages:

```bash
pip install -r requirements.txt
```

### Running the Test Script

Before running any of the main scripts, validate your environment with the test script:

```bash
python test_script.py
```

This will:
- ✅ Test data processing functions
- ✅ Validate statistical analysis capabilities
- ✅ Check date table creation functionality
- ✅ Test error handling and edge cases
- ✅ Run integration tests

### Main Scripts

1. **COVID-19 Forecasting Scripts**:
   - `Goolge Big Query Sandbox COVID Data_Forcast.py`
   - `Google Big Query COVID19 Sandback Prophet Foracast with lockdown.py`
   - `Google Big Query, COVID19 Sandbox Writeback Forcast.py`

2. **Birth Weight Analysis**:
   - `GBC Birth BMI Linier Regression.py`
   - `GBC CDC Birth BMI analysis.py`

3. **Date Table Creation**:
   - `Create Dim Tables/Create and make date table.py`
   - `Create Dim Tables/create date table in fabric.py`

## Note on Credentials

The scripts in this repository reference specific Google Cloud credentials. Before running the main scripts:

1. Set up your own Google Cloud project
2. Create a service account and download the JSON key
3. Update the credential paths in the scripts
4. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable

## Testing

The `test_script.py` provides comprehensive testing without requiring actual BigQuery credentials, making it perfect for:
- Validating your development environment
- Testing data processing logic
- Learning about the analysis techniques used
- Continuous integration testing
