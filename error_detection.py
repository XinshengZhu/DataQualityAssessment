import sys
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    row_number, monotonically_increasing_id, when, col, concat, lit,
    avg, stddev, abs, collect_list, udf, trim, struct, countDistinct
)
from pyspark.sql.window import Window
import jellyfish
from pyspark.sql.types import BooleanType


def null_detection(df):

    # Define the list of null values
    explicit_null_list = ['0', 'N/A', '#N/A', 'n/a', 'No Data', 'Not Applicable']
    disguised_null_list = ['01/01/1900', '(999) 999-9999']

    # Replace the null values with 'Unknown'
    df_null = df
    for column in df.columns:
        df_null = df_null.withColumn(column, 
                                     when(col(column).isNull() | col(column).cast("string").isin(explicit_null_list + disguised_null_list), 
                                          'Unknown').otherwise(col(column).cast("string")))
    
    # Keep only the rows that contain at least one known value
    df_null = df_null.filter(~reduce(lambda x, y: x & y, (col(column) != 'Unknown' for column in df.columns)))
    
    return df_null

def misspelling_detection(df, partition_columns, target_column, window_size=4, threshold=5):

    # Create a new column to flag the misspelling
    df_misspelling = df.withColumn('MISSPELLING', lit(''))

    # Remove the rows with null values in the partition columns and the target column
    df_misspelling = df_misspelling.na.drop(subset=partition_columns+[target_column])

    # Sort the dataframe by the target column
    df_misspelling = df_misspelling.sort(target_column)

    # Group the target column by the partition columns
    grouped = df_misspelling.groupBy(partition_columns).agg(collect_list(target_column).alias(target_column), collect_list('INDEX').alias('INDEX'))

    # Detect the misspelling by using the Sorted-Neighborhood algorithm
    for row in grouped.collect():
        partition = row.asDict()
        target_list = partition[target_column]
        index_list = partition['INDEX']
        for i in range(0, len(target_list) - window_size + 1, window_size - 1):
            flag = False
            temp = []
            temp.append(index_list[i])
            for j in range(i + 1, i + window_size):
                temp.append(index_list[j])
                if (
                    flag == False and
                    jellyfish.levenshtein_distance(target_list[i], target_list[j]) in range(1, threshold + 1)
                ):
                    flag = True
            if flag == True:
                temp_str = ' '.join([str(x) for x in temp])
                df_misspelling = df_misspelling.withColumn('MISSPELLING', when(col('INDEX').isin(temp), temp_str).otherwise(col('MISSPELLING')))

    # Keep only the rows that contain misspelling flags
    df_misspelling = df_misspelling.filter(col('MISSPELLING') != '').sort('MISSPELLING')

    return df_misspelling

def jaro_winkler_similarity(value, vocab):

    if value is None:
        return False
    if value not in vocab:
        for v in vocab:
            if jellyfish.jaro_winkler_similarity(value, v) < 0.9:
                return True
    return False

def vocabulary_misspelling_detection(df):

    # Create a new column to flag the misspelling compared to the vocabulary
    df_vocabulary_misspelling = df.withColumn('VOCAB', lit(''))

    # Define the list of columns that need to be checked
    nta_column = [column for column in df.columns if 'nta' in column.lower()][0]
    boro_column = [column for column in df.columns if 'boro' in column.lower()][0]

    # Define the list of vocabularies
    vocab_boro = ['MANHATTAN', 'BRONX', 'BROOKLYN', 'QUEENS', 'STATEN IS']
    with open('reference/NTAName.txt', 'r') as file:
        vocab_nta = file.read().splitlines()

    # Define the user-defined functions
    NTA_similarity_udf = udf(lambda x: jaro_winkler_similarity(x, vocab_nta), BooleanType())
    BORO_similarity_udf = udf(lambda x: jaro_winkler_similarity(x, vocab_boro), BooleanType())

    # Detect the vocabulary violations
    df_vocabulary_misspelling = df_vocabulary_misspelling.withColumn('VOCAB',
                                      when(col(nta_column).isNotNull() & NTA_similarity_udf(trim(col(nta_column))), 
                                           concat(col('VOCAB'), lit(','), lit(nta_column))).otherwise(col('VOCAB')))
    df_vocabulary_misspelling = df_vocabulary_misspelling.withColumn('VOCAB',
                                      when(col(boro_column).isNotNull() & BORO_similarity_udf(trim(col(boro_column))), 
                                           concat(col('VOCAB'), lit(','), lit(boro_column))).otherwise(col('VOCAB')))
    
    # Keep only the rows that contain vocabulary misspelling flags
    df_vocabulary_misspelling = df_vocabulary_misspelling.filter(col('VOCAB') != '')

    return df_vocabulary_misspelling
    
def outlier_detection(df):

    # Create a new column to flag the outlier
    df_outlier = df.withColumn('OUTLIER', lit(''))

    # Define the list of columns that need to be checked
    outlier_columns = [column for column in df.columns if 'total' in column.lower() or 'avg' in column.lower() or 'age' in column.lower() or 'grade' in column.lower()]

    # Detect the outlier
    for column in outlier_columns:
        result = df.agg(avg(column), stddev(column)).collect()
        avg_val = result[0][0]
        stddev_val = result[0][1]
        df_outlier = df_outlier.withColumn('OUTLIER', 
                                               when((abs((col(column) - avg_val)) / stddev_val > 2.2), 
                                                            concat(col('OUTLIER'), lit(','), lit(column))).otherwise(col('OUTLIER')))
        
    # Keep only the rows that contain outlier flags
    df_outlier = df_outlier.filter(col('OUTLIER') != '')

    return df_outlier

def pattern_violation_detection(df):

    # Create a new column to flag the regular expression violations
    df_pattern_violation = df.withColumn('REGEX', lit(''))

    # Define the list of columns that need to be checked
    date_columns = [column for column in df.columns if 'date' in column.lower()]
    phone_columns = [column for column in df.columns if 'phone' in column.lower()]
    email_columns = [column for column in df.columns if 'email' in column.lower()]
    zip_columns = [column for column in df.columns if 'zip' in column.lower() or 'post' in column.lower()]

    # Define the list of regular expressions
    regexp_date = r'^(0[1-9]|1[0-2])/(0[1-9]|[12][0-9]|3[01])/\d{4}$'
    regexp_phone = r'^\(\d{3}\) \d{3}-\d{4}$'
    regexp_email = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    regexp_zip = r'^\d{5}(-\d{4})?$'

    # Detect the regex pattern violation
    for column in date_columns:
        df_pattern_violation = df_pattern_violation.withColumn('REGEX', 
                                                 when(col(column).isNull() | col(column).rlike(regexp_date), 
                                                      col('REGEX')).otherwise(concat(col('REGEX'), lit(','), lit(column))))
    for column in phone_columns:
        df_pattern_violation = df_pattern_violation.withColumn('REGEX', 
                                                 when(col(column).isNull() | col(column).rlike(regexp_phone), 
                                                      col('REGEX')).otherwise(concat(col('REGEX'), lit(','), lit(column))))                                        
    for column in email_columns:
        df_pattern_violation = df_pattern_violation.withColumn('REGEX', 
                                                 when(col(column).isNull() | col(column).rlike(regexp_email), 
                                                      col('REGEX')).otherwise(concat(col('REGEX'), lit(','), lit(column))))
    for column in zip_columns:
        df_pattern_violation = df_pattern_violation.withColumn('REGEX', 
                                                 when(col(column).isNull() | col(column).rlike(regexp_zip), 
                                                      col('REGEX')).otherwise(concat(col('REGEX'), lit(','), lit(column))))
    
    # Keep only the rows that contain pattern violation flags
    df_pattern_violation = df_pattern_violation.filter((col('REGEX') != ''))

    return df_pattern_violation

def constriant_violation_detection(df, left, right):

    # Filter the rows without null values in the left and right columns
    df_constriant_violation = df.dropna(subset=left+right)

    struct_cols_right = [col(column) for column in right]
    struct_expr_right = struct(*struct_cols_right)
    
    # Group the right columns by the left columns
    # Count the distinct values of the right columns for each group
    grouped = df_constriant_violation.groupBy(left).agg(countDistinct(struct_expr_right).alias("distinct_count"))

    # Filter the groups with more than one distinct value
    grouped = grouped.filter(col("distinct_count") > 1)

    # Join the grouped dataframe with the original dataframe
    df_constriant_violation = df_constriant_violation.join(grouped, left, "inner").select(df_constriant_violation.columns).sort(left)

    return df_constriant_violation

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: error_detection.py <file>", file=sys.stderr)
        sys.exit(-1)
    
    # Get the file path
    file_path = sys.argv[1]

    # Get the file name
    file_name = file_path.split('/')[-1].split('.')[0]
    
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Load the data stored in the file
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Trim the leading and trailing whitespaces
    for column in df.columns:
        df = df.withColumn(column, trim(col(column)))

    # Add a unique index to each row
    df_with_index = df.withColumn('INDEX', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)

    # Detect null values
    df_null = null_detection(df_with_index)

    # Store the result in a new file
    df_null.write.csv(file_name + '_null_detection', header=True)

    # Detect misspelling
    latitude_column = [column for column in df.columns if 'latitude' in column.lower()][0]
    longitude_column = [column for column in df.columns if 'longitude' in column.lower()][0]
    partition_columns = [latitude_column, longitude_column]
    target_column = [column for column in df.columns if 'address' in column.lower() or 'street' in column.lower()][0]
    df_misspelling = misspelling_detection(df_with_index, partition_columns, target_column)

    # Store the result in a new file
    df_misspelling.write.csv(file_name + '_misspelling_detection', header=True)

    # Detect vocabulary misspelling
    df_vocabulary_misspelling = vocabulary_misspelling_detection(df_with_index)

    # Store the result in a new file
    df_vocabulary_misspelling.write.csv(file_name + '_vocabulary_misspelling_detection', header=True)

    # Detect outlier
    df_outlier = outlier_detection(df_with_index)

    # Store the result in a new file
    df_outlier.write.csv(file_name + '_outlier_detection', header=True)

    # Detect pattern violation
    df_pattern_violation = pattern_violation_detection(df_with_index)

    # Store the result in a new file
    df_pattern_violation.write.csv(file_name + '_pattern_violation_detection', header=True)

    # Detect constriant violation
    left = ['NTA']
    right = ['BORO']
    df_constriant_violation = constriant_violation_detection(df_with_index, left, right)

    # Store the result in a new file
    df_constriant_violation.write.csv(file_name + '_constriant_violation_detection', header=True)

    # Stop the SparkSession
    spark.stop()
