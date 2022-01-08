import subprocess
from pathlib import Path

cwd = Path(__file__).parent
srcs = ['edge-matched', 'cod', 'geoboundaries']

if __name__ == '__main__':
    for src in srcs:
        subprocess.run([
            's3cmd', 'sync',
            '--acl-public',
            cwd / f'outputs/{src}.json',
            f's3://data.fieldmaps.io/{src}.json',
        ])
        subprocess.run([
            's3cmd', 'sync',
            '--acl-public',
            '--delete-removed',
            '--rexclude', '\/\.',
            '--multipart-chunk-size-mb=5120',
            cwd / f'outputs/{src}',
            f's3://data.fieldmaps.io/',
        ])
