"""Append independent CDC files to daily partitioned Iceberg raw tables."""

from __future__ import annotations

import argparse
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import duckdb


CATALOG_ALIAS = "lake"
CATALOG_WAREHOUSE = "warehouse"
EXPECTED_SOURCES = ("payments", "receipts", "disbursements")


DOWNSTREAM_TABLES = (
    """
    CREATE TABLE IF NOT EXISTS lake.staging.stg_payments (
        payment_id VARCHAR,
        payment_amount DECIMAL(18, 2),
        payment_currency VARCHAR,
        sender_user_id VARCHAR,
        payment_status VARCHAR,
        payment_detailed_status VARCHAR,
        payment_error_code VARCHAR,
        payment_reason VARCHAR,
        payment_type VARCHAR,
        total_payment_intents INTEGER,
        request_state_code VARCHAR,
        has_dispute_chargeback BOOLEAN,
        payment_created_at TIMESTAMPTZ,
        payment_updated_at TIMESTAMPTZ,
        ingestion_run_id VARCHAR,
        ingested_at TIMESTAMPTZ,
        source_file_name VARCHAR,
        source_file_hash VARCHAR
    ) PARTITIONED BY (DAY(payment_created_at))
    """,
    """
    CREATE TABLE IF NOT EXISTS lake.staging.stg_receipts (
        receipt_id VARCHAR,
        destination_country_code VARCHAR,
        destination_currency VARCHAR,
        amount_charged DECIMAL(18, 2),
        fee_amount DECIMAL(18, 2),
        exchange_rate DECIMAL(18, 6),
        promotion_amount DECIMAL(18, 2),
        destination_amount DECIMAL(18, 2),
        sender_user_id VARCHAR,
        payment_id VARCHAR,
        disbursement_id VARCHAR,
        beneficiary_id VARCHAR,
        receipt_created_at TIMESTAMPTZ,
        ingestion_run_id VARCHAR,
        ingested_at TIMESTAMPTZ,
        source_file_name VARCHAR,
        source_file_hash VARCHAR
    ) PARTITIONED BY (DAY(receipt_created_at))
    """,
    """
    CREATE TABLE IF NOT EXISTS lake.staging.stg_disbursements (
        disbursement_id VARCHAR,
        disbursement_user_id VARCHAR,
        disbursement_type VARCHAR,
        disbursement_status VARCHAR,
        disbursement_raw_status VARCHAR,
        provider_status VARCHAR,
        disbursement_amount DECIMAL(18, 2),
        provider_name VARCHAR,
        beneficiary_name VARCHAR,
        destination_country VARCHAR,
        destination_currency VARCHAR,
        disbursement_created_at TIMESTAMPTZ,
        disbursement_updated_at TIMESTAMPTZ,
        ingestion_run_id VARCHAR,
        ingested_at TIMESTAMPTZ,
        source_file_name VARCHAR,
        source_file_hash VARCHAR
    ) PARTITIONED BY (DAY(disbursement_created_at))
    """,
    """
    CREATE TABLE IF NOT EXISTS lake.intermediate.int_receipts_enriched (
        receipt_id VARCHAR,
        payment_id VARCHAR,
        disbursement_id VARCHAR,
        sender_user_id VARCHAR,
        beneficiary_id VARCHAR,
        destination_country_code VARCHAR,
        destination_currency VARCHAR,
        amount_charged DECIMAL(18, 2),
        fee_amount DECIMAL(18, 2),
        exchange_rate DECIMAL(18, 6),
        promotion_amount DECIMAL(18, 2),
        destination_amount DECIMAL(18, 2),
        receipt_created_at TIMESTAMPTZ,
        payment_amount DECIMAL(18, 2),
        payment_currency VARCHAR,
        payment_status VARCHAR,
        payment_detailed_status VARCHAR,
        payment_type VARCHAR,
        has_dispute_chargeback BOOLEAN,
        payment_created_at TIMESTAMPTZ,
        payment_updated_at TIMESTAMPTZ,
        disbursement_user_id VARCHAR,
        disbursement_type VARCHAR,
        disbursement_status VARCHAR,
        disbursement_raw_status VARCHAR,
        provider_status VARCHAR,
        disbursement_amount DECIMAL(18, 2),
        provider_name VARCHAR,
        beneficiary_name VARCHAR,
        destination_country VARCHAR,
        disbursement_created_at TIMESTAMPTZ,
        disbursement_updated_at TIMESTAMPTZ,
        has_payment BOOLEAN,
        has_disbursement BOOLEAN,
        is_complete_remittance BOOLEAN,
        remittance_updated_at TIMESTAMPTZ
    ) PARTITIONED BY (DAY(receipt_created_at))
    """,
)


def sql_string(value: str) -> str:
    """Quote a trusted connection value for a DuckDB SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def file_hash(file_path: Path) -> str:
    """Return a SHA-256 fingerprint without loading the whole file into memory."""
    hasher = hashlib.sha256()
    with file_path.open("rb") as source_file:
        while chunk := source_file.read(1024 * 1024):
            hasher.update(chunk)
    return hasher.hexdigest()


def attach_catalog(connection: duckdb.DuckDBPyConnection) -> None:
    """Load Iceberg support and attach the local REST catalog for this session."""
    try:
        connection.execute("LOAD ICEBERG")
    except duckdb.Error:
        connection.execute("INSTALL ICEBERG")
        connection.execute("LOAD ICEBERG")

    endpoint = os.environ.get("ICEBERG_CATALOG_ENDPOINT", "http://localhost:8181")
    connection.execute(
        f"""
        ATTACH {sql_string(CATALOG_WAREHOUSE)} AS {CATALOG_ALIAS} (
            TYPE ICEBERG,
            ENDPOINT {sql_string(endpoint)},
            AUTHORIZATION_TYPE NONE,
            DISABLE_MULTI_TABLE_COMMIT TRUE
        )
        """
    )
    connection.execute("CREATE SCHEMA IF NOT EXISTS lake.raw")
    connection.execute("CREATE SCHEMA IF NOT EXISTS lake.ops")
    connection.execute("CREATE SCHEMA IF NOT EXISTS lake.staging")
    connection.execute("CREATE SCHEMA IF NOT EXISTS lake.intermediate")
    connection.execute("CREATE SCHEMA IF NOT EXISTS lake.marts")


def create_downstream_tables(connection: duckdb.DuckDBPyConnection) -> None:
    """Bootstrap empty partitioned Iceberg targets before dbt incremental MERGEs."""
    for statement in DOWNSTREAM_TABLES:
        connection.execute(statement)


def create_audit_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS lake.ops.ingestion_audit (
            audit_id VARCHAR NOT NULL,
            ingestion_run_id VARCHAR NOT NULL,
            source_name VARCHAR NOT NULL,
            source_file_name VARCHAR NOT NULL,
            source_file_hash VARCHAR NOT NULL,
            ingestion_ts TIMESTAMPTZ NOT NULL,
            status VARCHAR NOT NULL,
            row_count BIGINT,
            completed_ts TIMESTAMPTZ,
            error_message VARCHAR
        )
        """
    )


def table_exists(connection: duckdb.DuckDBPyConnection, relation: str) -> bool:
    try:
        connection.execute(f"SELECT 1 FROM {relation} LIMIT 0")
    except duckdb.Error:
        return False
    return True


def was_successfully_ingested(
    connection: duckdb.DuckDBPyConnection, source_name: str, source_hash: str
) -> bool:
    result = connection.execute(
        """
        SELECT 1
        FROM lake.ops.ingestion_audit
        WHERE source_name = ?
          AND source_file_hash = ?
          AND status = 'succeeded'
        LIMIT 1
        """,
        [source_name, source_hash],
    ).fetchone()
    return result is not None


def create_raw_table_if_needed(
    connection: duckdb.DuckDBPyConnection,
    source_name: str,
    csv_path: Path,
    ingestion_run_id: str,
    ingestion_ts: datetime,
    source_hash: str,
) -> None:
    relation = f"lake.raw.{source_name}"
    local_warehouse = os.environ.get("ICEBERG_LOCAL_WAREHOUSE_PATH")
    if local_warehouse:
        table_directory = Path(local_warehouse) / "raw" / source_name
        (table_directory / "metadata").mkdir(parents=True, exist_ok=True)
        (table_directory / "data").mkdir(parents=True, exist_ok=True)

    if table_exists(connection, relation):
        return

    connection.execute(
        f"""
        CREATE TABLE {relation} AS
        SELECT
            source_data.*,
            ? AS _ingestion_run_id,
            ? AS _ingested_at,
            ? AS _source_file_name,
            ? AS _source_file_hash
        FROM READ_CSV_AUTO(?, header = TRUE, all_varchar = TRUE) AS source_data
        WHERE FALSE
        """,
        [
            ingestion_run_id,
            ingestion_ts,
            csv_path.name,
            source_hash,
            str(csv_path),
        ],
    )
    connection.execute(
        f"ALTER TABLE {relation} SET PARTITIONED BY (DAY(_ingested_at))"
    )


def append_source_rows(
    connection: duckdb.DuckDBPyConnection,
    source_name: str,
    csv_path: Path,
    ingestion_run_id: str,
    ingestion_ts: datetime,
    source_hash: str,
) -> int:
    result = connection.execute(
        f"""
        INSERT INTO lake.raw.{source_name}
        SELECT
            source_data.*,
            ? AS _ingestion_run_id,
            ? AS _ingested_at,
            ? AS _source_file_name,
            ? AS _source_file_hash
        FROM READ_CSV_AUTO(?, header = TRUE, all_varchar = TRUE) AS source_data
        """,
        [
            ingestion_run_id,
            ingestion_ts,
            csv_path.name,
            source_hash,
            str(csv_path),
        ],
    ).fetchone()
    return result[0]


def load_sources(source_dir: Path, database_path: Path) -> None:
    available_sources = []
    for source_name in EXPECTED_SOURCES:
        csv_path = source_dir / f"{source_name}.csv"
        if csv_path.is_file():
            available_sources.append((source_name, csv_path.resolve()))

    if not available_sources:
        expected_files = "\n".join(
            f"  - {source_dir / f'{source_name}.csv'}"
            for source_name in EXPECTED_SOURCES
        )
        raise FileNotFoundError(f"No expected source files found:\n{expected_files}")

    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    ingestion_run_id = str(uuid4())
    ingestion_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    failures = []

    try:
        attach_catalog(connection)
        create_audit_table(connection)
        create_downstream_tables(connection)
        print(f"Ingestion run: {ingestion_run_id} at {ingestion_ts.isoformat()}")

        for source_name, csv_path in available_sources:
            source_hash = file_hash(csv_path)
            audit_id = str(uuid4())

            if was_successfully_ingested(connection, source_name, source_hash):
                connection.execute(
                    """
                    INSERT INTO lake.ops.ingestion_audit
                    VALUES (?, ?, ?, ?, ?, ?, 'skipped_already_ingested', 0, ?, NULL)
                    """,
                    [
                        audit_id,
                        ingestion_run_id,
                        source_name,
                        csv_path.name,
                        source_hash,
                        ingestion_ts,
                        ingestion_ts,
                    ],
                )
                print(f"Skipped lake.raw.{source_name}: file content was already ingested")
                continue

            connection.execute(
                """
                INSERT INTO lake.ops.ingestion_audit
                VALUES (?, ?, ?, ?, ?, ?, 'started', NULL, NULL, NULL)
                """,
                [
                    audit_id,
                    ingestion_run_id,
                    source_name,
                    csv_path.name,
                    source_hash,
                    ingestion_ts,
                ],
            )

            try:
                create_raw_table_if_needed(
                    connection,
                    source_name,
                    csv_path,
                    ingestion_run_id,
                    ingestion_ts,
                    source_hash,
                )
                connection.execute("BEGIN TRANSACTION")
                row_count = append_source_rows(
                    connection,
                    source_name,
                    csv_path,
                    ingestion_run_id,
                    ingestion_ts,
                    source_hash,
                )
                connection.execute(
                    """
                    UPDATE lake.ops.ingestion_audit
                    SET status = 'succeeded', row_count = ?, completed_ts = ?
                    WHERE audit_id = ?
                    """,
                    [row_count, datetime.now(timezone.utc), audit_id],
                )
                connection.execute("COMMIT")
                print(f"Loaded lake.raw.{source_name}: {row_count:,} rows")
            except Exception as error:
                try:
                    connection.execute("ROLLBACK")
                except duckdb.Error:
                    pass
                connection.execute(
                    """
                    UPDATE lake.ops.ingestion_audit
                    SET status = 'failed', completed_ts = ?, error_message = ?
                    WHERE audit_id = ?
                    """,
                    [datetime.now(timezone.utc), str(error), audit_id],
                )
                failures.append(source_name)
                print(f"Failed lake.raw.{source_name}: {error}")
    finally:
        connection.close()

    if failures:
        failed_sources = ", ".join(failures)
        raise RuntimeError(f"Ingestion failed for: {failed_sources}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory containing payments.csv, receipts.csv, and disbursements.csv.",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=Path("warehouse/felix.duckdb"),
        help="Local DuckDB control database used to attach the Iceberg catalog.",
    )
    args = parser.parse_args()
    load_sources(args.source_dir, args.database)


if __name__ == "__main__":
    main()
