pub enum ToolError {
    Db(DbError),
    Parse(String),
}

pub struct Widget {
    pub name: String,
    pub total: usize,
}

pub trait Render {
    type Output;
    const MIN: usize = 0;
    fn render(&self) -> Self::Output;
    fn reset(&mut self);
}

pub struct TupleWidget(String, usize);

pub struct Marker;

impl Widget {
    pub fn new(name: String) -> Self {
        Self { name, total: 0 }
    }

    pub fn render(&self) -> String {
        self.name.clone()
    }
}

impl Widget {
    pub fn count(&self) -> usize {
        self.total
    }
}

impl Render for Widget {
    fn render(&self) -> Self::Output {
        self.name.clone()
    }

    fn reset(&mut self) {}
}

impl<T> From<T> for Widget
where
    T: Into<String>,
{
    fn from(value: T) -> Self {
        Self::new(value.into())
    }
}

pub fn helper() {}
