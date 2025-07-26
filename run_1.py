import subprocess

# srcs = ["cod", "geoboundaries"]
srcs = ["cod"]

if __name__ == "__main__":
    for src in srcs:
        subprocess.run(["python", "-m", f"app.{src}.inputs"], check=False)
