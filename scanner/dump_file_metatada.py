from multiprocessing import Process, Queue
from pathlib import Path
from queue import Empty
from sys import argv

from .local_file_metadata import LocalFileMetadata, scan


def run(start_dir_path: Path):
    print(start_dir_path)
