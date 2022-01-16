import subprocess

funcs = ['extended', 'meta', 'edge_extender']

if __name__ == '__main__':
    for func in funcs:
        subprocess.run(['python3', '-m', f'processing.{func}'])
    subprocess.run(['python3', 'sync.py'])
