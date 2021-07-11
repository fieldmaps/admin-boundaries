from pathlib import Path
from subprocess import run

cwd = Path(__file__).parent
srcs = ['cod', 'geoboundaries']
metas = [*srcs, 'edge-matched']
grps = ['originals', 'normalized', 'standardized', 'extended', 'clipped']
outputs = ['humanitarian', 'open']

if __name__ == '__main__':
    for meta in metas:
        run([
            's3cmd', 'sync',
            '--acl-public',
            '--delete-removed',
            '--rexclude', '\/\.',
            (cwd / f'data/{meta}.json').resolve(),
            f's3://fieldmapsdata/{meta}.json',
        ])
    for output in outputs:
        run([
            's3cmd', 'sync',
            '--acl-public',
            '--delete-removed',
            '--rexclude', '\/\.',
            (cwd / f'data/edge-matched/{output}').resolve(),
            f's3://fieldmapsdata/edge-matched/',
        ])
    for grp in grps:
        for src in srcs:
            run([
                's3cmd', 'sync',
                '--acl-public',
                '--delete-removed',
                '--rexclude', '\/\.',
                (cwd / f'data/{src}/{grp}').resolve(),
                f's3://fieldmapsdata/{src}/',
            ])
