"""chq init — create persistent query_log archive table and materialized view."""

import sys

import click


_CREATE_TABLE = """\
CREATE TABLE IF NOT EXISTS {database}.chq_query_log
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, normalized_query_hash)
TTL event_date + INTERVAL 90 DAY
AS SELECT * FROM system.query_log WHERE 1 = 0\
"""

_CREATE_MV = """\
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.chq_query_log_mv
TO {database}.chq_query_log
AS SELECT * FROM system.query_log\
"""


@click.command("init")
@click.option("--host", envvar="CHQ_HOST", required=True, help="ClickHouse host.")
@click.option("--port", envvar="CHQ_PORT", default=8443, type=int, help="ClickHouse port (default: 8443).")
@click.option("--user", envvar="CHQ_USER", default="default", help="ClickHouse user (default: default).")
@click.option("--password", envvar="CHQ_PASSWORD", default="", help="ClickHouse password.")
@click.option("--secure/--no-secure", envvar="CHQ_SECURE", default=True, help="Use TLS (default: true).")
@click.option("--database", envvar="CHQ_DATABASE", default="default", help="Target database (default: default).")
def init_cmd(host, port, user, password, secure, database):
    """Create persistent query_log archive table and materialized view.

    Sets up a MergeTree table and materialized view so that query_log
    data survives ClickHouse Cloud scale-to-zero or replica replacement.
    """
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, secure=secure,
    )

    try:
        click.echo(f"Creating database {database} (if not exists)...")
        client.command(f"CREATE DATABASE IF NOT EXISTS {database}")

        needs_backfill = not client.command(f"EXISTS TABLE {database}.chq_query_log")

        click.echo(f"Creating table {database}.chq_query_log (if not exists)...")
        client.command(_CREATE_TABLE.format(database=database))

        if needs_backfill:
            cutoff = client.command("SELECT now()")

        click.echo(f"Creating materialized view {database}.chq_query_log_mv (if not exists)...")
        client.command(_CREATE_MV.format(database=database))

        if needs_backfill:
            click.echo("Backfilling existing query_log data...")
            client.command(
                f"INSERT INTO {database}.chq_query_log SELECT * FROM system.query_log WHERE event_time < {{cutoff:DateTime}}",
                parameters={"cutoff": cutoff},
            )
        else:
            click.echo("Already initialized, skipping backfill.")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    click.echo()
    click.echo("Done! Existing data backfilled and new queries will be archived automatically.")
    click.echo()
    click.echo("To use with chq:")
    click.echo(f"  chq --table {database}.chq_query_log --host {host}")
