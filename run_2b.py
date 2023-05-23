import subprocess

if __name__ == "__main__":
    subprocess.run(["python", "-m", "app.edge_matched"])
    subprocess.run(["python", "sync.py"])
