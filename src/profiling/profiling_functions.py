from functools import reduce as functools_reduce
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

DEFAULT_PLACEHOLDER_VALUES = {"UNKNOWN", "TBD", "-", "N/A", "NA", "NULL", "미분류", "기타", "ETC", ""}
DEFAULT_DATE_FORMATS = [
    "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ssXXX",
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd",
    "yyyy/MM/dd HH:mm", "yyyy/MM/dd", "yyyy.MM.dd"
]

PHONE_FORMAT_REGEX = r"^01[0-9]-[0-9]{3,4}-[0-9]{4}$"
EMAIL_FORMAT_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

def parsed_date_expr(column_name: str, date_formats: list = None):
    formats = date_formats or DEFAULT_DATE_FORMATS
    trimmed = F.trim(F.col(column_name))
    return F.coalesce(*[F.try_to_timestamp(trimmed, F.lit(fmt)) for fmt in formats])

def format_label_expr(target_col):
    col_obj = F.col(target_col) if isinstance(target_col, str) else target_col
    trimmed = F.trim(col_obj)
    pattern_map = [
        (r"^\d{8}$", "yyyyMMdd"),
        (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$", "yyyy-MM-dd HH:mm:ss"),
        (r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}([.+-].*)?$", "yyyy-MM-ddTHH:mm:ss(+TZ)"),
        (r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd"),
        (r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}$", "yyyy/MM/dd HH:mm"),
        (r"^\d{4}/\d{2}/\d{2}$", "yyyy/MM/dd"),
        (r"^\d{4}\.\d{2}\.\d{2}$", "yyyy.MM.dd"),
    ]
    chained = F.when(trimmed.rlike(pattern_map[0][0]), F.lit(pattern_map[0][1]))
    for regex, label in pattern_map[1:]:
        chained = chained.when(trimmed.rlike(regex), F.lit(label))
    return chained.otherwise(F.lit("OTHER"))

def profile_columns(df: DataFrame, row_count: int, column_roles: dict, date_formats: dict = None):
    date_formats = date_formats or {}
    results = []

    profile_schema = StructType([
        StructField("column_name", StringType(), False), StructField("column_role", StringType(), True),
        StructField("total_count", LongType(), True), StructField("null_count", LongType(), True),
        StructField("null_ratio", DoubleType(), True), StructField("blank_count", LongType(), True),
        StructField("blank_ratio", DoubleType(), True), StructField("distinct_count", LongType(), True),
        StructField("distinct_rate", DoubleType(), True), StructField("min_value", StringType(), True),
        StructField("max_value", StringType(), True), StructField("min_length", LongType(), True),
        StructField("max_length", LongType(), True), StructField("avg_length", DoubleType(), True),
        StructField("parse_success_count", LongType(), True), StructField("parse_fail_count", LongType(), True),
        StructField("format_distribution", StringType(), True),
    ])

    if row_count == 0 or not column_roles:
        return df.sparkSession.createDataFrame([], schema=profile_schema)

    for column_name, role in column_roles.items():
        raw_col = F.col(column_name)
        trimmed_col = F.trim(raw_col)

        base_agg = df.agg(
            F.sum(F.when(raw_col.isNull(), 1).otherwise(0)).alias("null_count"),
            F.sum(F.when(raw_col.isNotNull() & (trimmed_col == ""), 1).otherwise(0)).alias("blank_count"),
            F.countDistinct(column_name).alias("distinct_count"),
            F.min(F.length(raw_col)).alias("min_length"),
            F.max(F.length(raw_col)).alias("max_length"),
            F.avg(F.length(raw_col)).alias("avg_length"),
        ).collect()[0]

        null_count = int(base_agg["null_count"]) if base_agg["null_count"] else 0
        blank_count = int(base_agg["blank_count"]) if base_agg["blank_count"] else 0
        distinct_count = int(base_agg["distinct_count"]) if base_agg["distinct_count"] else 0

        null_ratio = round(null_count / row_count * 100, 2) if row_count else 0.0
        blank_ratio = round(blank_count / row_count * 100, 2) if row_count else 0.0
        distinct_rate = round(distinct_count / row_count * 100, 2) if row_count else 0.0

        min_length = int(base_agg["min_length"]) if base_agg["min_length"] is not None else None
        max_length = int(base_agg["max_length"]) if base_agg["max_length"] is not None else None
        avg_length = round(float(base_agg["avg_length"]), 2) if base_agg["avg_length"] is not None else None

        min_value = max_value = parse_success_count = parse_fail_count = format_distribution = None

        if role == "DATE_DATETIME":
            formats = date_formats.get(column_name, DEFAULT_DATE_FORMATS)
            parsed_expr = parsed_date_expr(column_name, formats)
            parse_df = df.filter(raw_col.isNotNull() & (trimmed_col != "")).select(
                raw_col.alias("raw_value"), parsed_expr.alias("parsed_value")
            )
            date_stats = parse_df.agg(
                F.sum(F.when(F.col("parsed_value").isNotNull(), 1).otherwise(0)).alias("parse_success_count"),
                F.sum(F.when(F.col("parsed_value").isNull(), 1).otherwise(0)).alias("parse_fail_count"),
                F.min("parsed_value").alias("min_value"), F.max("parsed_value").alias("max_value"),
            ).collect()[0]

            parse_success_count = int(date_stats["parse_success_count"]) if date_stats["parse_success_count"] else 0
            parse_fail_count = int(date_stats["parse_fail_count"]) if date_stats["parse_fail_count"] else 0
            min_value = str(date_stats["min_value"]) if date_stats["min_value"] is not None else None
            max_value = str(date_stats["max_value"]) if date_stats["max_value"] is not None else None

            format_dist_df = parse_df.withColumn("format", format_label_expr("raw_value")).groupBy("format").count()\
                .withColumn("ratio", F.round(F.col("count") / F.lit(row_count) * 100, 2)).orderBy(F.desc("count"))
            format_distribution = [r.asDict() for r in format_dist_df.collect()]
            min_length = max_length = avg_length = None

        elif role in ("GENERAL", "BUSINESS_KEY", "TECHNICAL_PK"):
            val_stats = df.filter(raw_col.isNotNull() & (trimmed_col != "")).agg(
                F.min(raw_col).alias("min_val"), F.max(raw_col).alias("max_val")
            ).collect()[0]
            min_value = str(val_stats["min_val"]) if val_stats["min_val"] is not None else None
            max_value = str(val_stats["max_val"]) if val_stats["max_val"] is not None else None

        elif role in ("PHONE", "EMAIL"):
            regex = PHONE_FORMAT_REGEX if role == "PHONE" else EMAIL_FORMAT_REGEX
            fmt_df = df.filter(raw_col.isNotNull() & (trimmed_col != "")).withColumn(
                "format", F.when(trimmed_col.rlike(regex), F.lit("MATCH_PATTERN")).otherwise(F.lit("UNMATCH_PATTERN"))
            ).groupBy("format").count().withColumn("ratio", F.round(F.col("count") / F.lit(row_count) * 100, 2)).orderBy(F.desc("count"))
            format_distribution = [r.asDict() for r in fmt_df.collect()]

        results.append((
            str(column_name), str(role), int(row_count), int(null_count), float(null_ratio),
            int(blank_count), float(blank_ratio), int(distinct_count), float(distinct_rate),
            min_value, max_value, min_length, max_length, avg_length,
            parse_success_count, parse_fail_count, str(format_distribution) if format_distribution is not None else None
        ))

    return df.sparkSession.createDataFrame(results, schema=profile_schema)

def profile_categorical(df: DataFrame, row_count: int, categorical_columns: list,
                        code_master: dict = None, expected_patterns: dict = None,
                        extra_placeholder_values: set = None, semantic_variant_groups: list = None):
    code_master = code_master or {}
    expected_patterns = expected_patterns or {}
    placeholder_values = DEFAULT_PLACEHOLDER_VALUES | (extra_placeholder_values or set())
    semantic_variant_groups = semantic_variant_groups or []
    unioned = None

    for column_name in categorical_columns:
        distribution_df = df.groupBy(column_name).count()\
            .withColumn("ratio", F.round(F.col("count") / F.lit(row_count) * 100, 2))\
            .withColumnRenamed(column_name, "value").withColumn("column_name", F.lit(column_name))\
            .select("column_name", "value", "count", "ratio")

        master_values = code_master.get(column_name)
        pattern = expected_patterns.get(column_name)
        variant_values = set()
        for group in semantic_variant_groups:
            if column_name in group.get("columns", []):
                variant_values |= set(group.get("values", []))

        def classify_value_type(value_col):
            expr = F.when(value_col.isNull(), F.lit("NULL"))\
                .when(F.trim(value_col) == "", F.lit("BLANK"))\
                .when(F.trim(value_col).isin(list(placeholder_values)), F.lit("PLACEHOLDER"))\
                .when(F.trim(value_col).isin(list(variant_values)) if variant_values else F.lit(False), F.lit("SEMANTIC_VARIANT"))\
                .when(F.trim(value_col).contains("999"), F.lit("RESERVED_CANDIDATE"))
            if master_values is not None:
                expr = expr.when(~F.trim(value_col).isin(list(master_values)), F.lit("MASTER_UNMATCH"))
            elif pattern is not None:
                expr = expr.when(~F.trim(value_col).rlike(pattern), F.lit("PATTERN_UNMATCH"))
            return expr.otherwise(F.lit("STANDARD_VALUE"))

        distribution_df = distribution_df.withColumn("value_category", classify_value_type(F.col("value")))
        unioned = distribution_df if unioned is None else unioned.unionByName(distribution_df)

    if unioned is None:
        return None, None

    unusual_candidates_df = unioned.filter(~F.col("value_category").isin(["STANDARD_VALUE", "NULL"])).orderBy("column_name", F.desc("count"))
    return unioned.orderBy("column_name", F.desc("count")), unusual_candidates_df

def profile_duplicates(df: DataFrame, row_count: int, key_columns: list):
    distinct_row_count = df.dropDuplicates().count()
    row_duplicate_count = row_count - distinct_row_count
    row_duplicate_ratio = round(row_duplicate_count / row_count * 100, 2) if row_count else 0.0

    duplicate_key_df = df.groupBy(*key_columns).count().filter(F.col("count") > 1).orderBy(F.desc("count")) if key_columns else df.sparkSession.createDataFrame([], "count INT")
    key_null_count = df.filter(functools_reduce(lambda a, b: a | b, [F.col(c).isNull() for c in key_columns])).count() if key_columns else 0

    dup_agg = duplicate_key_df.agg(
        F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicate_extra_count"),
        F.count("*").alias("duplicate_key_value_count"),
        F.coalesce(F.max("count"), F.lit(0)).alias("max_duplicate_count")
    ).collect()[0] if key_columns else {"duplicate_extra_count": 0, "duplicate_key_value_count": 0, "max_duplicate_count": 0}

    summary_rows = [
        ("key_columns", ",".join(key_columns)), ("row_count", row_count),
        ("distinct_row_count", distinct_row_count), ("row_duplicate_count", row_duplicate_count),
        ("row_duplicate_ratio", row_duplicate_ratio), ("key_null_count", key_null_count),
        ("duplicate_key_value_count", dup_agg["duplicate_key_value_count"]),
        ("duplicate_extra_count", dup_agg["duplicate_extra_count"]),
        ("max_duplicate_count", dup_agg["max_duplicate_count"]),
    ]
    return df.sparkSession.createDataFrame(summary_rows, ["metric", "value"]), duplicate_key_df

def profile_cross_column(df: DataFrame, row_count: int, cross_column_rules: list, date_formats: dict = None):
    if not cross_column_rules:
        return None
    date_formats = date_formats or {}
    unioned = None

    for rule in cross_column_rules:
        columns = rule["columns"]
        parsed_cols = []
        null_cond = None
        for c in columns:
            formats = date_formats.get(c, DEFAULT_DATE_FORMATS)
            p = parsed_date_expr(c, formats)
            parsed_cols.append(p)
            cond = F.col(c).isNull()
            null_cond = cond if null_cond is None else (null_cond | cond)

        chain_cond = None
        for a, b in zip(parsed_cols, parsed_cols[1:]):
            cond = a <= b
            chain_cond = cond if chain_cond is None else (chain_cond & cond)

        check_df = df.withColumn(
            "sequence_status",
            F.when(null_cond, F.lit("CONTAINS_NULL")).when(chain_cond, F.lit("ORDERED")).otherwise(F.lit("REVERSED"))
        ).groupBy("sequence_status").count().withColumn("target_columns", F.lit(" <= ".join(columns)))\
         .withColumn("ratio", F.round(F.col("count") / F.lit(row_count) * 100, 2)).select("target_columns", "sequence_status", "count", "ratio")

        unioned = check_df if unioned is None else unioned.unionByName(check_df)

    return unioned

def save_delta_and_csv(df: DataFrame, output_path: str, name: str):
    df.write.format("delta").mode("overwrite").save(f"{output_path}/{name}")
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(f"{output_path}/{name}_csv")

def save_single_csv(dbutils, df: DataFrame, output_path: str, filename: str):
    export_path = f"{output_path}/pm_export"
    temp_path = f"{export_path}/_temp_{filename}"
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(temp_path)

    files = dbutils.fs.ls(temp_path)
    part_file = [f.path for f in files if f.name.startswith("part-")][0]
    final_path = f"{export_path}/{filename}"
    dbutils.fs.mv(part_file, final_path)
    dbutils.fs.rm(temp_path, True)

# profiling_functions.py 중 save_profiling_results 부분 수정 또는 추가

def save_profiling_results(spark, dbutils, output_base_path: str, table_name: str, results: dict, ingest_date: str = None):
    output_path = f"{output_base_path}/{table_name}"
    filename_map = {
        "summary": "01_summary.csv",
        "column_profile": "02_column_profile.csv",
        "categorical_distribution": "03_categorical_distribution.csv",
        "duplicate_summary": "04_duplicate_summary.csv",
        "duplicate_key_rows": "04_duplicate_key_rows.csv",
        "cross_column_check": "05_cross_column_check.csv",
        "unusual_candidates": "06_unusual_candidates.csv",
    }

    current_timestamp = F.current_timestamp()

    for key, df in results.items():
        if df is None:
            continue
        
        # ingest_date와 executed_at 메타데이터 컬럼 주입
        if ingest_date and "ingest_date" not in df.columns:
            df = df.withColumn("ingest_date", F.lit(ingest_date))
        if "executed_at" not in df.columns:
            df = df.withColumn("executed_at", current_timestamp)

        save_delta_and_csv(df, output_path, key)
        if key in filename_map:
            save_single_csv(dbutils, df, output_path, filename_map[key])

    print(f"[프로파일링 결과 저장 완료] {table_name} -> {output_path} (ingest_date={ingest_date})")