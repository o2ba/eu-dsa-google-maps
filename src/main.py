from datetime import datetime, timedelta
import typer

from ingest_date import ingest_date_snowflake


def parse_date_range(rng: str) -> list[str]:
    """Parse YYYY-MM-DD:YYYY-MM-DD → list of iso date strings."""
    try:
        start_str, end_str = rng.split(":")
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_str, "%Y-%m-%d").date()
    except Exception as e:
        raise typer.BadParameter(f"Invalid --range value {rng}: {e}")

    if end < start:
        raise typer.BadParameter("End date must be >= start date")

    delta = (end - start).days
    return [(start + timedelta(days=i)).isoformat() for i in range(delta + 1)]


def main(
    date: str = typer.Option(None, "--date", "-d"),
    date_range: str = typer.Option(None, "--range", "-r"),
    target_platform: str = typer.Option("Google Maps", "--platform", "-p"),
):
    """
    Run daily or ranged ingestion into Snowflake.
    """

    # Validate arguments
    if (date is None) and (date_range is None):
        raise typer.BadParameter("Pass either --date or --range")
    if date and date_range:
        raise typer.BadParameter("Only specify one of --date or --range")

    # Single date ingestion
    if date:
        typer.echo(f"[ℹ] Starting ingestion for {date}")
        ingest_date_snowflake(date, target_platform)
        typer.echo(f"[✔] {date} complete")
        return

    # Range ingestion
    dates = parse_date_range(date_range)

    typer.echo(
        f"[ℹ] Processing {len(dates)} dates "
        f"from {dates[0]} → {dates[-1]}"
    )

    for d in dates:
        try:
            typer.echo(f"[ℹ] Starting ingestion for {d}")
            ingest_date_snowflake(d, target_platform)
            typer.echo(f"[✔] {d} complete")
        except Exception as e:
            typer.echo(f"[✘] {d} failed: {e}")


if __name__ == "__main__":
    typer.run(main)