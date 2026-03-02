import { readFile } from 'fs';

class Reader {
  read(path) {
    return readFile(path);
  }
}

function processData(data) {
  return data.trim();
}

const transform = (x) => x * 2;
