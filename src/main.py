from datetime import datetime, timedelta
from ingest_date import ingest_date
from concurrent.futures import ThreadPoolExecutor, as_completed
from ingest_date import SessionLocal
from repository.ledger import IngestionLedgerRepository

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
    date: str = typer.Option(None, "--date", "-d"),
    date_range: str = typer.Option(None, "--range", "-r"),
    target_platform: str = typer.Option("Google Maps", "--platform", "-p"),
    max_threads: int = typer.Option(5, "--max-threads"),
    skip_days: int = typer.Option(1, "--skip-days"),
    skip_existing: bool = typer.Option(False, "--skip-existing"),
):
    if (date is None) and (date_range is None):
        raise typer.BadParameter("Pass either --date or --range")
    if date and date_range:
        raise typer.BadParameter("Only specify one of --date or --range")

    if date:
        ingest_date(date, target_platform)
        return

    dates = parse_date_range(date_range)

    # apply skip-days
    if skip_days > 1:
        dates = dates[::skip_days]

    # filter out existing
    if skip_existing:
        from ingest_date import SessionLocal
        from repository.ledger import IngestionLedgerRepository

        with SessionLocal() as session:
            filtered = []
            for d in dates:
                existing = IngestionLedgerRepository.get_by_date(session, d)
                if existing and any(l.success for l in existing):
                    typer.echo(f"[⚠] Skipping {d}, already successful in DB")
                else:
                    filtered.append(d)
            dates = filtered

    typer.echo(
        f"[ℹ] Processing {len(dates)} dates from {dates[0]} → {dates[-1]} with {max_threads} threads"
    )

    # threaded ingestion
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(ingest_date, d, target_platform): d for d in dates}
        for fut in as_completed(futures):
            d = futures[fut]
            try:
                fut.result()
                typer.echo(f"[✔] {d} complete")
            except Exception as e:
                typer.echo(f"[✘] {d} failed: {e}")

if __name__ == "__main__":
    typer.run(main)