# task1_identify_departments_high_satisfaction.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round as spark_round

def initialize_spark(app_name="Task1_Identify_Departments"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the employee data from a CSV file into a Spark DataFrame.

    Parameters:
        spark (SparkSession): The SparkSession object.
        file_path (str): Path to the employee_data.csv file.

    Returns:
        DataFrame: Spark DataFrame containing employee data.
    """
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_departments_high_satisfaction(df):
    """
    Identify departments with more than 50% of employees having a Satisfaction Rating > 4 and Engagement Level 'High'.

    Parameters:
        df (DataFrame): Spark DataFrame containing employee data.

    Returns:
        DataFrame: DataFrame containing departments meeting the criteria with their respective percentages.
    """
    # TODO: Implement Task 1
    # Steps:
    # 1. Filter employees with SatisfactionRating > 4 and EngagementLevel == 'High'.
    # 2. Calculate the percentage of such employees within each department.
    # 3. Identify departments where this percentage exceeds 50%.
    # 4. Return the result DataFrame.
    filtered_df = df.filter((col('SatisfactionRating') > 4) & (col('EngagementLevel') == 'High'))
    
    # Step 2: Calculate the count of employees in each department
    total_count_df = df.groupBy('Department').agg(count('*').alias('TotalEmployees'))
    
    # Step 3: Calculate the count of employees meeting the condition in each department
    high_satisfaction_df = filtered_df.groupBy('Department').agg(count('*').alias('HighSatisfactionEmployees'))
    
    # Join the dataframes to calculate the percentage
    result_df = total_count_df.join(high_satisfaction_df, on='Department', how='left')
    
    # Percentage of employees with high satisfaction in each department
    result_df = result_df.withColumn('Percentage', 
                                     (col('HighSatisfactionEmployees') / col('TotalEmployees')) * 100)
    
    # Departments where the percentage is greater than 50%
    result_df = result_df.filter(col('Percentage') > 50).select('Department', spark_round('Percentage', 2).alias('Percentage'))
    
    return result_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.

    Parameters:
        result_df (DataFrame): Spark DataFrame containing the result.
        output_path (str): Path to save the output CSV file.

    Returns:
        None
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 1.
    """
    # Initialize Spark
    spark = initialize_spark()
    
    # Define file paths
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-Shreyash991/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-Shreyash991/outputs/task1/departments_high_satisfaction.csv"
    
    # Load data
    df = load_data(spark, input_file)
    
    # Perform Task 1
    result_df = identify_departments_high_satisfaction(df)
    
    # Write the result to CSV
    write_output(result_df, output_file)
    
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
