"""
Object model for the metadata of local filesystem objects.
"""

from dataclasses import asdict, astuple, dataclass, fields
from datetime import datetime
from grp import getgrgid
from hashlib import md5
from os import PathLike
from pathlib import Path
from pwd import getpwuid
from queue import Empty
from stat import filemode
from typing import Iterator, Union


BLOCK_SIZE = 65536

PathType = Union[PathLike, str]


@dataclass
class LocalFileMetadata:
    path: Path
    mode: str
    num_links: int
    num_bytes: int
    user: str
    group: str
    mtime: datetime
    md5: str = None

    @classmethod
    def from_path(cls, p: Path):
        s = p.stat(follow_symlinks=False)
        result = cls(
            p,
            filemode(s.st_mode),
            s.st_nlink,
            s.st_size,
            getpwuid(s.st_uid).pw_name,
            getgrgid(s.st_gid).gr_name,
            datetime.fromtimestamp(s.st_mtime),
        )
        return result

    @property
    def type_code(self):
        return self.mode[0]

    @property
    def astuple(self):
        return astuple(self)

    @property
    def asdict(self):
        return asdict(self)

    def compute_md5(self) -> None:
        if not self.path.is_file():
            self.md5 = "NA"
            return
        h = md5()
        with self.path.open("rb") as fin:
            while data := fin.read(BLOCK_SIZE):
                h.update(data)
        self.md5 = h.hexdigest()


def scan(starting_dir: PathType) -> Iterator[LocalFileMetadata]:
    top = Path(starting_dir)
    for p in top.glob("**/*"):
        yield LocalFileMetadata.from_path(p)
