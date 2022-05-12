import subprocess

funcs = ['meta', 'edge_matched']

if __name__ == '__main__':
    for func in funcs:
        subprocess.run(['python3', '-m', f'processing.{func}'])
    subprocess.run(['python3', 'sync.py'])
