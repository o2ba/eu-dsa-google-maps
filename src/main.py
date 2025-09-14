from datetime import datetime, timedelta
from ingest_date import ingest_date
from concurrent.futures import ThreadPoolExecutor, as_completed

import typer

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
    date: str = typer.Option(None, "--date", "-d", help="Single date YYYY-MM-DD"),
    date_range: str = typer.Option(
        None, "--range", "-r", help="Date range in format YYYY-MM-DD:YYYY-MM-DD"
    ),
    target_platform: str = typer.Option("Google Maps", "--platform", "-p"),
    max_threads: int = typer.Option(5, "--max-threads", help="Max threads"),
):
    if (date is None) and (date_range is None):
        raise typer.BadParameter("Pass either --date or --range")

    if date and date_range:
        raise typer.BadParameter("Only specify one of --date or --range")

    if date:
        # single date ingestion
        ingest_date(date, target_platform)
        return

    # date_range mode
    dates = parse_date_range(date_range)
    typer.echo(f"[ℹ] Processing {len(dates)} dates from {dates[0]} → {dates[-1]} with {max_threads} threads")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {
            executor.submit(ingest_date, d, target_platform): d for d in dates
        }
        for fut in as_completed(futures):
            d = futures[fut]
            try:
                fut.result()
                typer.echo(f"[✔] {d} complete")
            except Exception as e:
                typer.echo(f"[✘] {d} failed: {e}")

if __name__ == "__main__":
    typer.run(main)