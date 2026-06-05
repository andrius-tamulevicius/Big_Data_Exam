from __future__ import annotations

import urllib.error
import urllib.request
from pathlib import Path


CHUNK_SIZE = 1024 * 1024 * 8


def download_file(url: str, destination: Path, overwrite: bool = False) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and not overwrite:
        temporary_path = destination.with_suffix(destination.suffix + ".part")
        if temporary_path.exists():
            temporary_path.unlink()
        print(f"Dataset already exists")
        return destination

    temporary_path = destination.with_suffix(destination.suffix + ".part")
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "big-data-exam-ais-downloader/1.0"},
    )

    print(f"Downloading dataset")
    print(f"Target: {destination}")

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            with temporary_path.open("wb") as output:
                chunks = iter(lambda: response.read(CHUNK_SIZE), b"")

                for chunk_number, chunk in enumerate(chunks, 1):
                    output.write(chunk)

                    if chunk_number % 100 == 0:
                        downloaded_mb = chunk_number * CHUNK_SIZE / (1024 * 1024)
                        print(f"Downloaded about {downloaded_mb:.0f} MB")
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to download AIS archive: {exc}") from exc

    temporary_path.replace(destination)
    print(f"Download complete")
    return destination
