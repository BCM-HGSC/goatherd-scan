from multiprocessing import Process, Queue
from pathlib import Path
from queue import Empty
from sys import argv

from .local_file_metadata import LocalFileMetadata, scan


def run(start_dir_path: Path):
    for r in scan(start_dir_path):
        r.set_md5_from_contents()
        print(r.mode, r.md5, f"{r.num_bytes:7d}", r.path, sep="\t")
