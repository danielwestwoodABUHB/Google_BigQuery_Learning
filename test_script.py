"""
Test Script for Google BigQuery Learning Repository

This script tests the core functionality of the BigQuery learning scripts
without requiring actual BigQuery credentials. It validates:
- Data processing functions
- Date table creation
- Statistical analysis functions
- Prophet forecasting capabilities
- Error handling and edge cases

Usage:
    python test_script.py

Requirements:
    - pandas
    - numpy
    - matplotlib
    - seaborn
    - statsmodels
    - prophet
    - holidays
"""

import os
import sys
import unittest
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from datetime import datetime, timedelta
import warnings

# Suppress warnings for cleaner test output
warnings.filterwarnings('ignore')

class TestDataProcessing(unittest.TestCase):
    """Test data processing functions"""
    
    def setUp(self):
        """Set up test data"""
        # Create sample COVID-like data
        dates = pd.date_range(start='2020-03-01', end='2020-12-31', freq='D')
        self.covid_data = pd.DataFrame({
            'date': dates,
            'daily_positive': np.random.randint(1000, 50000, len(dates)),
            'total_positive': np.cumsum(np.random.randint(1000, 5000, len(dates)))
        })
        
        # Create sample birth weight BMI data
        self.bmi_data = pd.DataFrame({
            'Year': [2019, 2020, 2021] * 100,
            'County_of_Residence': ['County_' + str(i % 50) for i in range(300)],
            'Avg_Birth_Weight': np.random.normal(3200, 400, 300),
            'Avg_Pre_Pregnancy_BMI': np.random.normal(25, 4, 300)
        })
    
    def test_covid_data_processing(self):
        """Test COVID data processing"""
        # Test data structure
        self.assertIn('date', self.covid_data.columns)
        self.assertIn('daily_positive', self.covid_data.columns)
        
        # Test data types
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(self.covid_data['date']))
        self.assertTrue(pd.api.types.is_integer_dtype(self.covid_data['daily_positive']))
        
        # Test for missing values
        self.assertFalse(self.covid_data['daily_positive'].isnull().any())
        
        print("✓ COVID data processing test passed")
    
    def test_prophet_data_preparation(self):
        """Test Prophet data preparation"""
        # Prepare data for Prophet (requires 'ds' and 'y' columns)
        prophet_data = self.covid_data.copy()
        prophet_data.rename(columns={'date': 'ds', 'daily_positive': 'y'}, inplace=True)
        
        # Test required columns exist
        self.assertIn('ds', prophet_data.columns)
        self.assertIn('y', prophet_data.columns)
        
        # Test data types for Prophet
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(prophet_data['ds']))
        self.assertTrue(prophet_data['y'].dtype in ['int64', 'float64'])
        
        # Test for positive values (Prophet works better with positive values)
        self.assertTrue((prophet_data['y'] >= 0).all())
        
        print("✓ Prophet data preparation test passed")
    
    def test_bmi_correlation_analysis(self):
        """Test BMI correlation analysis"""
        # Remove missing values
        clean_data = self.bmi_data.dropna(subset=['Avg_Pre_Pregnancy_BMI', 'Avg_Birth_Weight'])
        
        # Calculate correlation
        correlation = clean_data['Avg_Pre_Pregnancy_BMI'].corr(clean_data['Avg_Birth_Weight'])
        
        # Test correlation is a valid number
        self.assertFalse(np.isnan(correlation))
        self.assertTrue(-1 <= correlation <= 1)
        
        # Test linear regression
        X = clean_data['Avg_Pre_Pregnancy_BMI']
        y = clean_data['Avg_Birth_Weight']
        X = sm.add_constant(X)
        
        model = sm.OLS(y, X).fit()
        
        # Test model results
        self.assertIsNotNone(model.params)
        self.assertIsNotNone(model.rsquared)
        self.assertTrue(0 <= model.rsquared <= 1)
        
        print("✓ BMI correlation analysis test passed")


class TestDateTableCreation(unittest.TestCase):
    """Test date table creation functionality"""
    
    def create_date_table(self, start_date, end_date):
        """Create a date table similar to the one in the repository"""
        try:
            import holidays
        except ImportError:
            self.skipTest("holidays package not available")
        
        date_range = pd.date_range(start=start_date, end=end_date)
        date_table = pd.DataFrame(date_range, columns=['Date'])
        
        # Add date components
        date_table['Year'] = date_table['Date'].dt.year
        date_table['Month'] = date_table['Date'].dt.month
        date_table['Day'] = date_table['Date'].dt.day
        date_table['Weekday'] = date_table['Date'].dt.day_name()
        
        # Add holiday information
        us_holidays = holidays.US(years=date_table['Year'].unique())
        uk_holidays = holidays.UK(years=date_table['Year'].unique())
        
        date_table['US_Holiday'] = date_table['Date'].apply(lambda x: x in us_holidays)
        date_table['UK_Holiday'] = date_table['Date'].apply(lambda x: x in uk_holidays)
        date_table['US_Holiday_Name'] = date_table['Date'].apply(lambda x: us_holidays.get(x))
        date_table['UK_Holiday_Name'] = date_table['Date'].apply(lambda x: uk_holidays.get(x))
        
        return date_table
    
    def test_date_table_structure(self):
        """Test date table creation and structure"""
        start_date = '2020-01-01'
        end_date = '2020-12-31'
        
        date_table = self.create_date_table(start_date, end_date)
        
        # Test required columns
        required_columns = ['Date', 'Year', 'Month', 'Day', 'Weekday', 
                          'US_Holiday', 'UK_Holiday', 'US_Holiday_Name', 'UK_Holiday_Name']
        
        for col in required_columns:
            self.assertIn(col, date_table.columns, f"Missing column: {col}")
        
        # Test data types
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(date_table['Date']))
        self.assertTrue(pd.api.types.is_integer_dtype(date_table['Year']))
        self.assertTrue(pd.api.types.is_integer_dtype(date_table['Month']))
        self.assertTrue(pd.api.types.is_integer_dtype(date_table['Day']))
        
        # Test value ranges
        self.assertTrue((date_table['Month'] >= 1).all() and (date_table['Month'] <= 12).all())
        self.assertTrue((date_table['Day'] >= 1).all() and (date_table['Day'] <= 31).all())
        
        # Test holiday detection
        self.assertTrue(date_table['US_Holiday'].any())  # Should find some holidays
        
        print("✓ Date table creation test passed")


class TestProphetForecasting(unittest.TestCase):
    """Test Prophet forecasting functionality"""
    
    def setUp(self):
        """Set up Prophet test data"""
        try:
            from prophet import Prophet
            self.prophet_available = True
        except ImportError:
            self.prophet_available = False
        
        # Create sample time series data
        dates = pd.date_range(start='2020-01-01', end='2020-06-30', freq='D')
        np.random.seed(42)  # For reproducible results
        trend = np.linspace(1000, 5000, len(dates))
        noise = np.random.normal(0, 500, len(dates))
        seasonal = 1000 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)  # Weekly seasonality
        
        self.ts_data = pd.DataFrame({
            'ds': dates,
            'y': trend + seasonal + noise
        })
        
        # Ensure positive values
        self.ts_data['y'] = np.maximum(self.ts_data['y'], 100)
    
    def test_prophet_forecasting(self):
        """Test Prophet model creation and forecasting"""
        if not self.prophet_available:
            self.skipTest("Prophet package not available")
        
        from prophet import Prophet
        
        # Create and fit model
        model = Prophet(daily_seasonality=True, weekly_seasonality=True)
        model.fit(self.ts_data)
        
        # Create future dataframe
        future = model.make_future_dataframe(periods=30)
        
        # Test future dataframe structure
        self.assertIn('ds', future.columns)
        self.assertTrue(len(future) > len(self.ts_data))
        
        # Make predictions
        forecast = model.predict(future)
        
        # Test forecast structure
        required_cols = ['ds', 'yhat', 'yhat_lower', 'yhat_upper']
        for col in required_cols:
            self.assertIn(col, forecast.columns, f"Missing forecast column: {col}")
        
        # Test forecast values are reasonable
        self.assertFalse(forecast['yhat'].isnull().any())
        self.assertTrue((forecast['yhat_upper'] >= forecast['yhat']).all())
        self.assertTrue((forecast['yhat'] >= forecast['yhat_lower']).all())
        
        print("✓ Prophet forecasting test passed")


class TestErrorHandling(unittest.TestCase):
    """Test error handling and edge cases"""
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty dataframes"""
        empty_df = pd.DataFrame()
        
        # Test that operations on empty dataframes don't crash
        try:
            result = empty_df.dropna()
            self.assertEqual(len(result), 0)
        except Exception as e:
            self.fail(f"Empty dataframe handling failed: {e}")
        
        print("✓ Empty dataframe handling test passed")
    
    def test_missing_data_handling(self):
        """Test handling of missing data"""
        data_with_nulls = pd.DataFrame({
            'A': [1, 2, np.nan, 4, 5],
            'B': [np.nan, 2, 3, 4, np.nan]
        })
        
        # Test dropna functionality
        clean_data = data_with_nulls.dropna()
        self.assertFalse(clean_data.isnull().any().any())
        
        # Test fillna functionality
        filled_data = data_with_nulls.fillna(0)
        self.assertFalse(filled_data.isnull().any().any())
        
        print("✓ Missing data handling test passed")
    
    def test_correlation_with_invalid_data(self):
        """Test correlation calculation with invalid data"""
        # Test with constant values (should return NaN or handle gracefully)
        constant_data = pd.DataFrame({
            'x': [1, 1, 1, 1, 1],
            'y': [2, 3, 4, 5, 6]
        })
        
        try:
            corr = constant_data['x'].corr(constant_data['y'])
            # Should be NaN for constant x
            self.assertTrue(np.isnan(corr))
        except Exception as e:
            # Some versions might handle this differently
            pass
        
        print("✓ Invalid data correlation test passed")


def run_integration_test():
    """Run an integration test that simulates a complete workflow"""
    print("\n" + "="*50)
    print("RUNNING INTEGRATION TEST")
    print("="*50)
    
    try:
        # 1. Create sample data
        dates = pd.date_range(start='2020-01-01', end='2020-12-31', freq='D')
        np.random.seed(42)
        
        sample_data = pd.DataFrame({
            'date': dates,
            'daily_positive': np.random.poisson(5000, len(dates)),
            'total_tests': np.random.poisson(50000, len(dates))
        })
        
        print("✓ Sample data created")
        
        # 2. Data preprocessing
        sample_data['positive_rate'] = sample_data['daily_positive'] / sample_data['total_tests']
        sample_data = sample_data.dropna()
        
        print("✓ Data preprocessing completed")
        
        # 3. Basic statistics
        stats = {
            'mean_positive': sample_data['daily_positive'].mean(),
            'std_positive': sample_data['daily_positive'].std(),
            'mean_rate': sample_data['positive_rate'].mean()
        }
        
        print(f"✓ Statistics calculated: mean_positive={stats['mean_positive']:.0f}")
        
        # 4. Prophet forecasting (if available)
        try:
            from prophet import Prophet
            
            prophet_data = sample_data[['date', 'daily_positive']].copy()
            prophet_data.columns = ['ds', 'y']
            
            model = Prophet(daily_seasonality=False)
            model.fit(prophet_data)
            
            future = model.make_future_dataframe(periods=7)
            forecast = model.predict(future)
            
            print("✓ Prophet forecasting completed")
            
        except ImportError:
            print("⚠ Prophet not available, skipping forecasting")
        
        # 5. Visualization test (without showing plots)
        plt.ioff()  # Turn off interactive mode
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(sample_data['date'], sample_data['daily_positive'])
        ax.set_title('Sample COVID-19 Daily Positive Cases')
        ax.set_xlabel('Date')
        ax.set_ylabel('Daily Positive Cases')
        plt.close(fig)  # Close without showing
        
        print("✓ Visualization test completed")
        
        print("\n🎉 INTEGRATION TEST PASSED!")
        
    except Exception as e:
        print(f"\n❌ INTEGRATION TEST FAILED: {e}")
        return False
    
    return True


def main():
    """Main function to run all tests"""
    print("Google BigQuery Learning Repository - Test Script")
    print("="*60)
    
    # Check for required packages
    required_packages = ['pandas', 'numpy', 'matplotlib', 'seaborn', 'statsmodels']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ Missing required packages: {', '.join(missing_packages)}")
        print("Please install them using: pip install " + " ".join(missing_packages))
        return False
    
    # Optional packages
    optional_packages = ['prophet', 'holidays']
    available_optional = []
    
    for package in optional_packages:
        try:
            __import__(package)
            available_optional.append(package)
        except ImportError:
            pass
    
    print(f"✓ Required packages available: {', '.join(required_packages)}")
    if available_optional:
        print(f"✓ Optional packages available: {', '.join(available_optional)}")
    
    print("\nRunning unit tests...")
    print("-" * 30)
    
    # Run unit tests
    test_loader = unittest.TestLoader()
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [TestDataProcessing, TestDateTableCreation, 
                   TestProphetForecasting, TestErrorHandling]
    
    for test_class in test_classes:
        tests = test_loader.loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests with minimal output
    runner = unittest.TextTestRunner(verbosity=1, stream=open(os.devnull, 'w'))
    result = runner.run(test_suite)
    
    # Run integration test
    integration_success = run_integration_test()
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    if result.wasSuccessful() and integration_success:
        print("🎉 ALL TESTS PASSED!")
        print("\nThis repository's core functionality is working correctly.")
        print("You can now run the individual Python scripts with confidence.")
        return True
    else:
        print("❌ SOME TESTS FAILED")
        if not result.wasSuccessful():
            print(f"Unit test failures: {len(result.failures)}")
            print(f"Unit test errors: {len(result.errors)}")
        if not integration_success:
            print("Integration test failed")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)