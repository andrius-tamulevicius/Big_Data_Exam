import shutil
from pathlib import Path


def marker_path(path: Path) -> Path:
    return path.parent / f"{path.name}.complete"


def stage_done(path: Path) -> bool:
    return path.exists() and marker_path(path).exists()


def prepare_stage(path: Path) -> bool:
    marker = marker_path(path)

    if path.exists() and marker.exists():
        print(f"Stage already complete: {path}")
        return False

    if marker.exists():
        marker.unlink()

    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

    path.parent.mkdir(parents=True, exist_ok=True)
    return True


def finish_stage(path: Path) -> None:
    marker = marker_path(path)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("complete", encoding="utf-8")
