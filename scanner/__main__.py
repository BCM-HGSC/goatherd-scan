from pathlib import Path
from sys import argv

from .dump_file_metatada import run


def main():
    _, start_dir = argv
    start_dir_path = Path(start_dir)
    run(start_dir_path)


if __name__ == "__main__":
    main()
