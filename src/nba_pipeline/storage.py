from pathlib import Path


def land_raw(json_str: str, base_path: str, entity: str, key: str) -> str:
    """Write one raw API response to the volume and return its path.

    The file name comes from the key (a date or game ID), so re-running for the same key overwrites the same file instead of creating a duplicate.
    """
    out_dir = Path(base_path) / entity
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{key}.json"
    out_file.write_text(json_str, encoding="utf-8")
    return str(out_file)