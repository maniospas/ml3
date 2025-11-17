import venv
import os
import subprocess


def create_environment_file(logger, functions, dependencies: set[str]):
    """
    Creates .ml3/<tuple>.txt and corresponding venv.
    Returns (requirements_path, venv_name).
    """
    if not os.path.exists(".ml3"):
        os.makedirs(".ml3")

    tuple_name = "-".join(sorted(fn.__name__ for fn in functions))
    req_path = f".ml3/{tuple_name}.txt"
    venv_name = tuple_name

    if logger:
        logger.info("Running tuple: "+tuple_name)

    # Existing environment
    if os.path.exists(req_path):
        with open(req_path, "r") as f:
            first = f.readline().strip()
            if first.startswith("# venv: "):
                return req_path, first[len("# venv: "):]

    # Write requirements file
    with open(req_path, "w") as f:
        f.write(f"# venv: {venv_name}\n")
        for dep in sorted(dependencies):
            f.write(dep + "\n")

    # Create venv if missing
    venv_path = f".ml3/{venv_name}"
    if not os.path.exists(venv_path):
        builder = venv.EnvBuilder(with_pip=True)
        builder.create(venv_path)

    python = (
        os.path.join(venv_path, "bin", "python")
        if os.name != "nt"
        else os.path.join(venv_path, "Scripts", "python.exe")
    )

    # Print scrolling pip status
    print("\033[96m⟲\033[0m Installing dependencies")
    GRAY = "\033[90m"
    RESET = "\033[0m"

    proc = subprocess.Popen(
        [python, "-m", "pip", "install", "-r", req_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    lines = []
    for line in proc.stdout:
        line = line.rstrip()
        lines.append(line)
        if len(lines) > 8:
            lines.pop(0)
        if lines:
            print(f"\033[{len(lines)}A", end="")
        for l in lines:
            print("\033[2K", end="")
            print(f"{GRAY}│ {l}{RESET}")

    proc.wait()

    if proc.returncode != 0:
        if logger:
            logger.error(f"pip install failed for {venv_name}")
    else:
        if logger:
            logger.ok(f"Finished installing into venv: {venv_name}")

    return req_path, venv_name