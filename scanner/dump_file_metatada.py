from multiprocessing import Process, Queue
from pathlib import Path
from queue import Empty
from sys import stdout

from .local_file_metadata import LocalFileMetadata, scan


def run(start_dir_path: Path):
    print(*LocalFileMetadata.field_names(), sep="\t")
    stdout.flush()
    for r in scan(start_dir_path):
        r.set_md5_from_contents()
        print(*r.astuple, sep="\t")
