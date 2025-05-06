import pandas as pd
import logging


def load_data(file_path):
    """
    Load and preprocess internship data from CSV file.

    Args:
        file_path (str): Path to the CSV file

    Returns:
        pandas.DataFrame: Preprocessed DataFrame
    """
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully loaded data with {len(df)} records")

        # Clean data
        # Handle missing values
        df = df.fillna({'min_salary': 0, 'max_salary': 0, 'avg_salary': 0})

        # Convert salary columns to numeric
        for col in ['min_salary', 'max_salary', 'avg_salary']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Calculate average salary if missing
        mask = (df['avg_salary'] == 0) & (df['min_salary']
                                          > 0) & (df['max_salary'] > 0)
        df.loc[mask, 'avg_salary'] = (df.loc[mask, 'min_salary'] +
                                      df.loc[mask, 'max_salary']) / 2

        # Consolidate locations
        df['Location'] = df['Location'].str.split(',').str[0]

        # Clean job titles for better categorization
        df['Job Title'] = df['Job Title'].str.strip()

        return df

    except Exception as e:
        logging.error(f"Error in data processing: {e}")
        return pd.DataFrame()


def get_top_domains(df, limit=10):
    """
    Get the top domains (job titles) by count.

    Args:
        df (pandas.DataFrame): Input DataFrame
        limit (int): Number of top domains to return

    Returns:
        dict: Top domains with counts
    """
    domain_counts = df['Job Title'].value_counts().head(limit).to_dict()
    return domain_counts


def get_salary_insights(df, limit=10):
    """
    Get salary insights by domain.

    Args:
        df (pandas.DataFrame): Input DataFrame
        limit (int): Number of insights to return

    Returns:
        dict: Salary insights by domain
    """
    salary_insights = df.groupby('Job Title')['avg_salary'].mean().sort_values(
        ascending=False).head(limit).to_dict()
    return salary_insights


def get_jobs_by_city(df, limit=10):
    """
    Get job distribution by city.

    Args:
        df (pandas.DataFrame): Input DataFrame
        limit (int): Number of cities to return

    Returns:
        dict: Job counts by city
    """
    city_counts = df['Location'].value_counts().head(limit).to_dict()
    return city_counts
