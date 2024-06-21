import subprocess
from pathlib import Path

cwd = Path(__file__).parent
srcs = ["edge-matched", "cod", "geoboundaries", "gb-humanitarian"]
exts = ["json", "csv", "xlsx"]


def sync(src, dest):
    subprocess.run(
        [
            "rclone",
            "sync",
            "--exclude=.*",
            "--progress",
            "--s3-no-check-bucket",
            "--s3-chunk-size=256M",
            src,
            dest,
        ]
    )


def copy(src, dest):
    subprocess.run(
        [
            "rclone",
            "copyto",
            "--s3-no-check-bucket",
            "--s3-chunk-size=256M",
            src,
            dest,
        ]
    )


if __name__ == "__main__":
    subprocess.run(["python", "-m", "app.meta"])
    for src in srcs:
        sync(cwd / f"outputs/{src}", f"r2://fieldmaps-data/{src}")
        for ext in exts:
            copy(cwd / f"outputs/{src}.{ext}", f"r2://fieldmaps-data/{src}.{ext}")
    for ext in exts:
        copy(
            cwd / f"outputs/global-pcodes.{ext}",
            f"r2://fieldmaps-data/global-pcodes.{ext}",
        )
