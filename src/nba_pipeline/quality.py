class DataQualityError(Exception):
    """Raised when one or more data quality checks fail."""


def enforce_checks(results: dict, total_rows: int, label: str) -> None:
    """Print a PASS/FAIL line per check, then raise if anything failed.

    results:    {check_name: number_of_rows_that_broke_the_rule}
    total_rows: how many rows were checked
    label:      the table name, used in messages
    """
    # A gate needs a floor: an empty input would otherwise look like a clean pass
    if total_rows == 0:
        raise DataQualityError(f"{label}: no rows to check (empty input)")

    print(f"{label}: checked {total_rows} rows\n")

    failed = {}
    for name, n_bad in results.items():
        n_bad = n_bad or 0   # treat None (an empty sum) as zero offending rows
        print(f"{'PASS' if n_bad == 0 else 'FAIL'}  {name}: {n_bad}")
        if n_bad != 0:
            failed[name] = n_bad

    # Raising stops the notebook here, so the write cell after it never runs
    if failed:
        raise DataQualityError(f"{label}: {len(failed)} check(s) failed: {failed}")

    print(f"\n{label}: all checks passed")