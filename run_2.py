import subprocess

srcs = ["cod", "geoboundaries"]
funcs = ["originals", "standardize"]

if __name__ == "__main__":
    for src in srcs:
        for func in funcs:
            subprocess.run(["python", "-m", f"app.{src}.{func}"])
    subprocess.run(["python", "-m", "app.cod.gb_humanitarian"])
