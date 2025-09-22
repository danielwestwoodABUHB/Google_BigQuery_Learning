"""
Demo Script for Google BigQuery Learning Repository

This script demonstrates the core functionality available in the repository
using sample data (no BigQuery credentials required).

Usage:
    python demo_script.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from datetime import datetime

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

def create_sample_covid_data():
    """Create sample COVID-19 data similar to what would come from BigQuery"""
    print("Creating sample COVID-19 data...")
    
    dates = pd.date_range(start='2020-03-01', end='2020-12-31', freq='D')
    np.random.seed(42)  # For reproducible results
    
    # Simulate realistic COVID data with trends
    base_cases = 5000
    trend = np.linspace(1, 3, len(dates))  # Increasing trend
    seasonal = 0.3 * np.sin(2 * np.pi * np.arange(len(dates)) / 7)  # Weekly pattern
    noise = np.random.normal(0, 0.2, len(dates))
    
    daily_positive = base_cases * trend * (1 + seasonal + noise)
    daily_positive = np.maximum(daily_positive, 100).astype(int)  # Ensure positive integers
    
    data = pd.DataFrame({
        'date': dates,
        'daily_positive': daily_positive,
        'cumulative_positive': np.cumsum(daily_positive)
    })
    
    print(f"✓ Created {len(data)} days of sample data")
    return data

def analyze_covid_trends(data):
    """Analyze COVID trends similar to repository scripts"""
    print("\nAnalyzing COVID-19 trends...")
    
    # Basic statistics
    stats = {
        'total_cases': data['cumulative_positive'].iloc[-1],
        'avg_daily_cases': data['daily_positive'].mean(),
        'peak_daily_cases': data['daily_positive'].max(),
        'peak_date': data.loc[data['daily_positive'].idxmax(), 'date']
    }
    
    print(f"📊 Total cases: {stats['total_cases']:,}")
    print(f"📊 Average daily cases: {stats['avg_daily_cases']:.0f}")
    print(f"📊 Peak daily cases: {stats['peak_daily_cases']:,} on {stats['peak_date'].strftime('%Y-%m-%d')}")
    
    return stats

def create_sample_birth_data():
    """Create sample birth weight and BMI data"""
    print("\nCreating sample birth weight and BMI data...")
    
    np.random.seed(42)
    n_samples = 1000
    
    # Create realistic correlations between BMI and birth weight
    bmi = np.random.normal(25, 4, n_samples)  # Normal BMI distribution
    bmi = np.clip(bmi, 15, 45)  # Reasonable BMI range
    
    # Birth weight correlated with BMI (higher BMI -> slightly higher birth weight)
    birth_weight = 3200 + 15 * (bmi - 25) + np.random.normal(0, 400, n_samples)
    birth_weight = np.clip(birth_weight, 1500, 5000)  # Reasonable birth weight range
    
    data = pd.DataFrame({
        'Avg_Pre_Pregnancy_BMI': bmi,
        'Avg_Birth_Weight': birth_weight,
        'County': [f'County_{i%50}' for i in range(n_samples)],
        'Year': np.random.choice([2019, 2020, 2021], n_samples)
    })
    
    print(f"✓ Created {len(data)} birth records")
    return data

def analyze_bmi_correlation(data):
    """Analyze BMI and birth weight correlation"""
    print("\nAnalyzing BMI and birth weight correlation...")
    
    # Calculate correlation
    correlation = data['Avg_Pre_Pregnancy_BMI'].corr(data['Avg_Birth_Weight'])
    print(f"📊 Correlation coefficient: {correlation:.3f}")
    
    # Perform linear regression
    import statsmodels.api as sm
    X = data['Avg_Pre_Pregnancy_BMI']
    y = data['Avg_Birth_Weight']
    X = sm.add_constant(X)
    
    model = sm.OLS(y, X).fit()
    print(f"📊 R-squared: {model.rsquared:.3f}")
    print(f"📊 BMI coefficient: {model.params[1]:.1f} grams per BMI unit")
    
    return correlation, model

def create_date_table_demo():
    """Demonstrate date table creation"""
    print("\nCreating date table...")
    
    try:
        import holidays
        
        # Create date range
        dates = pd.date_range(start='2020-01-01', end='2020-12-31', freq='D')
        date_table = pd.DataFrame({'Date': dates})
        
        # Add date components
        date_table['Year'] = date_table['Date'].dt.year
        date_table['Month'] = date_table['Date'].dt.month
        date_table['Day'] = date_table['Date'].dt.day
        date_table['Weekday'] = date_table['Date'].dt.day_name()
        
        # Add holidays
        us_holidays = holidays.US(years=[2020])
        date_table['US_Holiday'] = date_table['Date'].apply(lambda x: x in us_holidays)
        date_table['Holiday_Name'] = date_table['Date'].apply(lambda x: us_holidays.get(x))
        
        holiday_count = date_table['US_Holiday'].sum()
        print(f"✓ Created date table with {len(date_table)} days")
        print(f"✓ Found {holiday_count} US holidays")
        
        return date_table
        
    except ImportError:
        print("⚠ Holidays package not available, creating basic date table")
        dates = pd.date_range(start='2020-01-01', end='2020-12-31', freq='D')
        date_table = pd.DataFrame({
            'Date': dates,
            'Year': dates.year,
            'Month': dates.month,
            'Day': dates.day,
            'Weekday': dates.day_name()
        })
        print(f"✓ Created basic date table with {len(date_table)} days")
        return date_table

def create_visualizations(covid_data, birth_data):
    """Create sample visualizations"""
    print("\nCreating visualizations...")
    
    # Set up the plotting style
    plt.style.use('default')
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    
    # 1. COVID daily cases over time
    ax1.plot(covid_data['date'], covid_data['daily_positive'], color='blue', linewidth=1)
    ax1.set_title('COVID-19 Daily Positive Cases')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Daily Cases')
    ax1.tick_params(axis='x', rotation=45)
    
    # 2. COVID cumulative cases
    ax2.plot(covid_data['date'], covid_data['cumulative_positive'], color='red', linewidth=2)
    ax2.set_title('COVID-19 Cumulative Cases')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Cumulative Cases')
    ax2.tick_params(axis='x', rotation=45)
    
    # 3. BMI vs Birth Weight scatter plot
    ax3.scatter(birth_data['Avg_Pre_Pregnancy_BMI'], birth_data['Avg_Birth_Weight'], 
                alpha=0.6, color='green')
    ax3.set_title('Pre-Pregnancy BMI vs Birth Weight')
    ax3.set_xlabel('Average Pre-Pregnancy BMI')
    ax3.set_ylabel('Average Birth Weight (grams)')
    
    # Add regression line
    z = np.polyfit(birth_data['Avg_Pre_Pregnancy_BMI'], birth_data['Avg_Birth_Weight'], 1)
    p = np.poly1d(z)
    ax3.plot(birth_data['Avg_Pre_Pregnancy_BMI'], 
             p(birth_data['Avg_Pre_Pregnancy_BMI']), "r--", alpha=0.8)
    
    # 4. Birth weight distribution
    ax4.hist(birth_data['Avg_Birth_Weight'], bins=30, alpha=0.7, color='purple')
    ax4.set_title('Birth Weight Distribution')
    ax4.set_xlabel('Birth Weight (grams)')
    ax4.set_ylabel('Frequency')
    
    plt.tight_layout()
    plt.savefig('demo_analysis_plots.png', dpi=150, bbox_inches='tight')
    plt.close()  # Close to avoid display issues in headless environment
    
    print("✓ Visualizations saved as 'demo_analysis_plots.png'")

def main():
    """Main demo function"""
    print("=" * 60)
    print("Google BigQuery Learning Repository - Demo Script")
    print("=" * 60)
    
    # 1. COVID-19 Analysis Demo
    covid_data = create_sample_covid_data()
    covid_stats = analyze_covid_trends(covid_data)
    
    # 2. Birth Weight Analysis Demo
    birth_data = create_sample_birth_data()
    correlation, model = analyze_bmi_correlation(birth_data)
    
    # 3. Date Table Demo
    date_table = create_date_table_demo()
    
    # 4. Create Visualizations
    create_visualizations(covid_data, birth_data)
    
    # Summary
    print("\n" + "=" * 60)
    print("DEMO SUMMARY")
    print("=" * 60)
    print("🎉 Successfully demonstrated:")
    print("   ✓ COVID-19 time series analysis")
    print("   ✓ Birth weight and BMI correlation analysis")
    print("   ✓ Date table creation with holidays")
    print("   ✓ Data visualization techniques")
    print("   ✓ Statistical modeling and regression")
    
    print(f"\n📈 Key findings from demo data:")
    print(f"   • COVID peak cases: {covid_stats['peak_daily_cases']:,}")
    print(f"   • BMI-Birth Weight correlation: {correlation:.3f}")
    print(f"   • Date table contains {len(date_table)} records")
    
    print("\n💡 This demonstrates the type of analysis you can perform")
    print("   with real BigQuery data using the scripts in this repository!")

if __name__ == "__main__":
    main()