import subprocess
import shutil
import os
from pathlib import Path


def build_lambda_package():
    # Create build directory
    build_dir = Path("build")
    dist_dir = Path("dist")

    # Clean previous builds
    if build_dir.exists():
        shutil.rmtree(build_dir)
    if dist_dir.exists():
        shutil.rmtree(dist_dir)

    build_dir.mkdir()
    dist_dir.mkdir()

    # Install dependencies
    subprocess.check_call([
        "pip",
        "install",
        "-r", "requirements.txt",
        "-t", str(build_dir)
    ])

    # Copy lambda code
    shutil.copy2("index.py", build_dir)

    # Create zip file
    shutil.make_archive(
        str(dist_dir / "rotation-lambda"),
        "zip",
        build_dir
    )


if __name__ == "__main__":
    build_lambda_package()
