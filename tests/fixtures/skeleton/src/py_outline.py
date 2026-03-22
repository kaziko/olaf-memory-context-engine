from dataclasses import dataclass


class FileProcessor:
    default_encoding: str = "utf-8"

    def __init__(self, path: str):
        self.path = path
        if path:
            self.size = 0  # nested — must NOT be extracted

    def process(self):
        pass


@dataclass
class Config:
    host: str
    port: int = 8080


def read_file(path: str) -> str:
    return ""
