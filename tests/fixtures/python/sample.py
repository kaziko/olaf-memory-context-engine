import os
import sys as system
from pathlib import Path

class FileProcessor:
    def process(self, path):
        return os.path.exists(path)

    def validate(self, path):
        return Path(path).is_file()

def read_file(path):
    with open(path) as f:
        return f.read()
