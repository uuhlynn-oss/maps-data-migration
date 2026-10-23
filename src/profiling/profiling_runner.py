import sys
import os

# maps/ 디렉터리를 sys.path에 추가하여 src 패키지를 인식시킴
try:
    _maps_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
except NameError:
    _maps_root = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))

if _maps_root not in sys.path:
    sys.path.insert(0, _maps_root)


from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from src.profiling.profiling_config import (
    BRONZE_CATALOG,
    BRONZE_SCHEMA,
    PROFILING_OUTPUT_BASE_PATH,
    SOURCE_CONFIG,
    get_all_table_names,
)

from src.profiling.profiling_functions import (
    profile_categorical,
    profile_columns,
    profile_cross_column,
    profile_duplicates,
    save_profiling_results,
    save_delta_and_csv,
    save_single_csv,
)

from src.profiling.integrated_profiling_builder import run_integrated_builder


TABLE_NAMES = get_all_table_names()
INTEGRATED_OUTPUT_PATH = f"{PROFILING_OUTPUT_BASE_PATH}/integrated"


def run_profiling_for_table(
    spark,
    dbutils,
    table_name: str,
    table_config: dict,
    ingest_date: str,
    output_base_path: str = PROFILING_OUTPUT_BASE_PATH,
):
    """
    UC Bronze 관리형 테이블을 직접 조회하여 프로파일링 수행.

    ingest_date:
        현재 프로파일링 대상 Bronze 배치의 ingest_date.
        개별 프로파일링 결과에도 동일한 값을 저장한다.
    """

    if not ingest_date:
        raise ValueError(
            f"❌ [{table_name}] 프로파일링 실행에 ingest_date가 필요합니다."
        )

    full_table_name = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    # Bronze 조회
    df = spark.table(full_table_name)

    row_count = df.count()
    column_count = len(df.columns)

    print("=" * 60)
    print(
        f"Profiling Start: {full_table_name} "
        f"(rows={row_count:,}, "
        f"columns={column_count}, "
        f"ingest_date={ingest_date})"
    )
    print("=" * 60)

    column_roles = table_config["column_roles"]
    key_columns = table_config["key_columns"]
    date_formats = table_config.get("date_formats", {})

    # =========================================================
    # 1. 컬럼 기본 통계 프로파일링
    # =========================================================
    column_profile_df = profile_columns(
        df,
        row_count,
        column_roles,
        date_formats,
    )

    # 개별 프로파일링 결과에 Bronze ingest_date 추가
    column_profile_df = column_profile_df.withColumn(
        "ingest_date",
        F.lit(ingest_date),
    )

    # =========================================================
    # 2. 범주형 분포 프로파일링
    # =========================================================
    categorical_columns = [
        c for c, r in column_roles.items()
        if r == "CODE"
    ]

    code_master = {}

    loader = table_config.get("code_master_loader")

    if loader:
        try:
            code_master = loader(spark, dbutils)
        except Exception as e:
            print(
                f"[경고] {table_name} 코드마스터 로딩 실패: {e}"
            )

    categorical_distribution_df = None
    unusual_candidates_df = None

    if categorical_columns:
        (
            categorical_distribution_df,
            unusual_candidates_df,
        ) = profile_categorical(
            df,
            row_count,
            categorical_columns,
            code_master=code_master,
            expected_patterns=table_config.get(
                "expected_patterns",
                {},
            ),
            extra_placeholder_values=table_config.get(
                "extra_placeholder_values",
                set(),
            ),
            semantic_variant_groups=table_config.get(
                "semantic_variant_groups",
                [],
            ),
        )

        # 개별 프로파일링 결과에 Bronze ingest_date 추가
        if categorical_distribution_df is not None:
            categorical_distribution_df = (
                categorical_distribution_df.withColumn(
                    "ingest_date",
                    F.lit(ingest_date),
                )
            )

        if unusual_candidates_df is not None:
            unusual_candidates_df = (
                unusual_candidates_df.withColumn(
                    "ingest_date",
                    F.lit(ingest_date),
                )
            )

    # =========================================================
    # 3. 키 중복 프로파일링
    # =========================================================
    duplicate_summary_df, duplicate_key_rows_df = profile_duplicates(
        df,
        row_count,
        key_columns,
    )

    # 개별 프로파일링 결과에 Bronze ingest_date 추가
    if duplicate_summary_df is not None:
        duplicate_summary_df = duplicate_summary_df.withColumn(
            "ingest_date",
            F.lit(ingest_date),
        )

    if duplicate_key_rows_df is not None:
        duplicate_key_rows_df = duplicate_key_rows_df.withColumn(
            "ingest_date",
            F.lit(ingest_date),
        )

    # =========================================================
    # 4. 시간 선후관계 프로파일링
    # =========================================================
    cross_column_check_df = profile_cross_column(
        df,
        row_count,
        table_config.get("cross_column_rules", []),
        date_formats,
    )

    # 개별 프로파일링 결과에 Bronze ingest_date 추가
    if cross_column_check_df is not None:
        cross_column_check_df = cross_column_check_df.withColumn(
            "ingest_date",
            F.lit(ingest_date),
        )

    # =========================================================
    # 5. 요약 테이블 생성
    # =========================================================
    summary_rows = [
        ("table_name", str(table_name)),
        ("row_count", str(row_count)),
        ("column_count", str(column_count)),
        ("business_key_columns", str(",".join(key_columns))),
        ("categorical_column_count", str(len(categorical_columns))),
        ("ingest_date", str(ingest_date)),
    ]

    summary_df = spark.createDataFrame(
        summary_rows,
        schema=StructType(
            [
                StructField("metric", StringType(), False),
                StructField("value", StringType(), True),
            ]
        ),
    )

    # =========================================================
    # 6. 프로파일링 결과 묶기
    # =========================================================
    results = {
        "summary": summary_df,
        "column_profile": column_profile_df,
        "categorical_distribution": categorical_distribution_df,
        "duplicate_summary": duplicate_summary_df,
        "duplicate_key_rows": duplicate_key_rows_df,
        "cross_column_check": cross_column_check_df,
        "unusual_candidates": unusual_candidates_df,
    }

    # =========================================================
    # 7. 결과 저장
    # =========================================================
    save_profiling_results(
        spark,
        dbutils,
        output_base_path,
        table_name,
        results,
        ingest_date=ingest_date,
    )

    # =========================================================
    # 8. 실행 결과 반환
    # =========================================================
    return {
        "table_name": table_name,
        "row_count": row_count,
        "ingest_date": ingest_date,
        "column_profile": column_profile_df,
    }


def run_all_sources(
    spark,
    dbutils,
    ingest_date: str = None,
    source_config: dict = SOURCE_CONFIG,
    output_base_path: str = PROFILING_OUTPUT_BASE_PATH,
):
    """
    전체 소스 프로파일링 실행.

    ingest_date가 지정되지 않은 경우:
        Bronze의 첫 번째 대상 테이블에서
        MAX(ingest_date)를 조회하여 최신 배치를 자동 선택한다.

    선택된 ingest_date는 모든 개별 프로파일링과
    통합 프로파일링에 동일하게 전달한다.
    """

    # =========================================================
    # 1. ingest_date 미지정 시 Bronze 최신 날짜 자동 감지
    # =========================================================
    if not ingest_date:
        try:
            first_table_name = list(source_config.keys())[0]

            full_table_name = (
                f"{BRONZE_CATALOG}."
                f"{BRONZE_SCHEMA}."
                f"{first_table_name}"
            )

            print(
                f"🔍 [자동 감지] Bronze 테이블 "
                f"({full_table_name})에서 최신 ingest_date 탐색 중..."
            )

            query = f"""
                SELECT MAX(ingest_date) AS max_ingest_date
                FROM {full_table_name}
            """

            row = spark.sql(query).collect()

            if row and row[0]["max_ingest_date"] is not None:
                ingest_date = str(row[0]["max_ingest_date"])

                print(
                    f"✨ [자동 감지 성공] "
                    f"Bronze 기준 최신 ingest_date: {ingest_date}"
                )

            else:
                raise ValueError(
                    f"Bronze 테이블 ({full_table_name})에서 "
                    f"유효한 ingest_date를 찾을 수 없습니다."
                )

        except Exception as e:
            raise RuntimeError(
                f"❌ [오류] Bronze 기반 ingest_date 자동 감지 실패: {e}"
            ) from e

    # =========================================================
    # 2. 최종 ingest_date 확인
    # =========================================================
    print("=" * 60)
    print(
        f"🚀 전체 소스 프로파일링 파이프라인 시작 "
        f"(Target Ingest Date: {ingest_date})"
    )
    print("=" * 60)

    # =========================================================
    # 3. 소스별 개별 프로파일링
    # =========================================================
    all_results = []

    for table_name, config in source_config.items():

        res = run_profiling_for_table(
            spark,
            dbutils,
            table_name,
            config,
            ingest_date=ingest_date,
            output_base_path=output_base_path,
        )

        all_results.append(res)

    # =========================================================
    # 4. 통합 프로파일링
    # =========================================================
    print("=" * 60)
    print(
        "통합 프로파일링(Integrated Profiling) 생성 및 "
        f"카탈로그 적재 시작 (ingest_date={ingest_date})"
    )
    print("=" * 60)

    run_integrated_builder(
        spark,
        dbutils,
        ingest_date=ingest_date,
    )

    # =========================================================
    # 5. 전체 결과 반환
    # =========================================================
    return all_results