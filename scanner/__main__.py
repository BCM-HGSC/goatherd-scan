from pathlib import Path
from sys import argv

from .dump_file_metatada import run


def main():
    print(argv)
    _, start_dir = argv
    start_dir_path = Path(start_dir).resolve()
    run(start_dir_path)


if __name__ == "__main__":
    main()