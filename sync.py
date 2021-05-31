from pathlib import Path
import subprocess

cwd = Path(__file__).parent
srcs = ['cod', 'geoboundaries']
grps = ['extended', 'clipped']
outputs = ['open', 'humanitarian']

if __name__ == '__main__':
    subprocess.run([
        's3cmd', 'sync',
        '--acl-public',
        '--delete-removed',
        '--dry-run',
        '--rexclude', '\/\.',
        (cwd / f'data/adm0').resolve(),
        f's3://fieldmapsdata/',
    ])
    for output in outputs:
        subprocess.run([
            's3cmd', 'sync',
            '--acl-public',
            '--delete-removed',
            '--rexclude', '\/\.',
            (cwd / f'data/edge-matched/{output}').resolve(),
            f's3://fieldmapsdata/edge-matched/',
        ])
    for grp in grps:
        for src in srcs:
            subprocess.run([
                's3cmd', 'sync',
                '--acl-public',
                '--delete-removed',
                '--rexclude', '\/\.',
                (cwd / f'data/{src}/{grp}').resolve(),
                f's3://fieldmapsdata/{src}/',
            ])
