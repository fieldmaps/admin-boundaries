import subprocess
from pathlib import Path

cwd = Path(__file__).parent
srcs = {
    "edge-matched": ["humanitarian", "open"],
    "cod": ["extended", "originals"],
    "geoboundaries": ["extended", "originals"],
    "gb-humanitarian": ["originals"],
}
exts = ["json", "csv", "xlsx"]


def upload(src, ext):
    subprocess.run(
        [
            "s3cmd",
            "sync",
            "--acl-public",
            cwd / f"outputs/{src}.{ext}",
            f"s3://data.fieldmaps.io/{src}.{ext}",
        ]
    )


if __name__ == "__main__":
    subprocess.run(["python", "-m", "app.meta"])
    for ext in exts:
        upload("global-pcodes", ext)
    for src in srcs:
        for ext in exts:
            upload(src, ext)
        for grp in srcs[src]:
            subprocess.run(
                [
                    "s3cmd",
                    "sync",
                    "--acl-public",
                    "--delete-removed",
                    "--rexclude",
                    r"\/\.",
                    "--multipart-chunk-size-mb=5120",
                    cwd / f"outputs/{src}/{grp}",
                    f"s3://data.fieldmaps.io/{src}/",
                ]
            )
