import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T

def profile_dataframe(df, max_categories=50):
    """
    Build a profiling dataframe:
    - column_name
    - null_count
    - unique_count
    - value_counts (as dict, for categorical/string)
    - min_value (for numeric/date/timestamp)
    - max_value (for numeric/date/timestamp)
    """
    results = []

    for field in df.schema.fields:
        col = field.name
        dtype = field.dataType

        # Null count
        null_count = df.filter(F.col(col).isNull()).count()

        # Unique values.
        unique_values = df.select(col).distinct().count()

        # Value counts.
        value_counts_dict = None
        vc = (
            df.groupBy(col)
                .count()
                .orderBy(F.desc("count"))
                .limit(max_categories)
                .collect()
        )
        value_counts_dict = {str(row[col]): row["count"] for row in vc if row[col] is not None}

        # Min/Max (only for numeric/date/timestamp)
        min_val = max_val = None
        if isinstance(dtype, (T.NumericType, T.DateType, T.TimestampType)):
            agg = df.agg(F.min(col).alias("min"), F.max(col).alias("max")).collect()[0]
            min_val, max_val = agg["min"], agg["max"]

        results.append({
            "column": col,
            "null_count": null_count,
            "unique_count": unique_values,
            "value_counts": value_counts_dict,
            "min_value": min_val,
            "max_value": max_val
        })

    # Convert to Spark DataFrame
    return pd.DataFrame(results)