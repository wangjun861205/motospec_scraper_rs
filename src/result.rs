use std::collections::HashMap;
use std::error::Error;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

pub enum Log<T, E> {
    Log(T),
    Err(T, E),
}

pub enum LogLevel {
    Brand(Brand),
    Model(Model),
    Spec(Spec),
}

#[derive(Debug, Clone)]
pub struct Brand {
    name: String,
    url: String,
}

impl Brand {
    pub fn new(name: String, url: String) -> Self {
        Self { name, url }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_url(&self) -> &str {
        &self.url
    }
}

#[derive(Debug, Clone)]
pub struct Model {
    brand: String,
    name: String,
    year: String,
    url: String,
}

impl Model {
    pub fn new(brand: String, name: String, year: String, url: String) -> Self {
        Self { brand, name, year, url }
    }

    pub fn get_brand(&self) -> &str {
        &self.brand
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_year(&self) -> &str {
        &self.year
    }

    pub fn get_url(&self) -> &str {
        &self.url
    }
}

#[derive(Debug, Clone)]
pub struct Spec {
    brand: String,
    model: String,
    year: String,
    specs: HashMap<String, String>,
}

impl Spec {
    pub fn new(brand: String, model: String, year: String) -> Self {
        Self {
            brand,
            model,
            year,
            specs: HashMap::new(),
        }
    }

    pub fn add_spec(&mut self, key: String, val: String) {
        self.specs.insert(key, val);
    }

    pub fn get_specs(&self) -> &HashMap<String, String> {
        &self.specs
    }

    pub fn get_brand(&self) -> &str {
        &self.brand
    }

    pub fn get_model(&self) -> &str {
        &self.model
    }

    pub fn get_year(&self) -> &str {
        &self.year
    }
}
