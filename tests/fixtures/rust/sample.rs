use std::path::Path;

struct FileReader {
    path: String,
}

trait Readable {
    fn read(&self) -> String;
}

impl FileReader {
    fn new(path: String) -> Self {
        FileReader { path }
    }

    fn exists(&self) -> bool {
        Path::new(&self.path).exists()
    }
}
