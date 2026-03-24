from dataclasses import dataclass
from functools import cached_property


class FileProcessor:
    default_encoding: str = "utf-8"

    def __init__(self, path: str):
        self.path = path
        if path:
            self.size = 0  # nested — must NOT be extracted

    def process(self):
        pass

    @property
    def filename(self) -> str:
        return self.path.split("/")[-1]

    @cached_property
    def extension(self):
        return self.path.rsplit(".", 1)[-1]


@dataclass
class Config:
    host: str
    port: int = 8080


def read_file(path: str) -> str:
    return ""
