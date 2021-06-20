use crate::crawler::{Logger, Store};
use crate::result::{Brand, Log, LogLevel, Model, Spec};
use mongodb::bson::{doc, to_bson, Document};
use mongodb::sync::{Client, Collection};
use std::error::Error;
use std::iter::FromIterator;

use crate::result::Result;

pub struct MongoStore(Collection<Document>);

impl MongoStore {
    pub fn new(database: &str, collection: &str) -> Result<Self> {
        let client = Client::with_uri_str("mongodb://wangjun:Wt20110523@127.0.0.1:27017/?compressors=disabled")?;
        let db = client.database(database);
        let col: Collection<Document> = db.collection(collection);
        Ok(Self(col))
    }
}

impl Store for MongoStore {
    fn insert_spec(&mut self, spec: &Spec) -> Result<()> {
        let specs = spec.get_specs();
        let doc = Document::from_iter(specs.into_iter().map(|(key, val)| (key.to_owned(), to_bson(val).unwrap())));
        self.0.insert_one(doc, None).map(|_| ()).map_err(|e| e.into())
    }
}

pub struct MongoLog(Collection<Document>);

impl MongoLog {
    pub fn new(database: &str, collection: &str) -> Result<Self> {
        let client = Client::with_uri_str("mongodb://wangjun:Wt20110523@127.0.0.1:27017/?compressors=disabled")?;
        let db = client.database(database);
        let col: Collection<Document> = db.collection(collection);
        Ok(Self(col))
    }
}

pub static COMPLETED: &str = "Completed";
pub static FAILED: &str = "Failed";
pub static BRAND: &str = "Brand";
pub static MODEL: &str = "Model";
pub static SPEC: &str = "Spec";

impl Logger for MongoLog {
    fn insert_log(&mut self, log: Log<LogLevel, Box<dyn Error + Send + Sync>>) -> Result<()> {
        match log {
            Log::Log(level) => match level {
                LogLevel::Brand(brand) => {
                    self.0.insert_one(
                        doc! {
                            "level": BRAND,
                            "state": COMPLETED,
                            "content": format!("brand: {}, url: {}", brand.get_name(), brand.get_url()),
                        },
                        None,
                    )?;
                }
                LogLevel::Model(model) => {
                    self.0.insert_one(
                        doc! {
                            "level": MODEL,
                            "state": COMPLETED,
                            "content": format!("brand: {}, model: {}, year: {}, url: {}", model.get_brand(), model.get_name(), model.get_year(), model.get_url()),
                        },
                        None,
                    )?;
                }
                LogLevel::Spec(spec) => {
                    self.0.insert_one(
                        doc! {
                            "level": SPEC,
                            "state": COMPLETED,
                            "content": format!("brand: {}, model: {}, year: {}", spec.get_brand(), spec.get_model(), spec.get_year()),
                        },
                        None,
                    )?;
                }
            },
            Log::Err(level, err) => match level {
                LogLevel::Brand(brand) => {
                    self.0.insert_one(
                        doc! {
                            "level": BRAND,
                            "state": FAILED,
                            "brand": brand.get_name(),
                            "url": brand.get_url(),
                            "error": err.to_string(),
                            "retry_count": 0,
                        },
                        None,
                    )?;
                }
                LogLevel::Model(model) => {
                    self.0.insert_one(
                        doc! {
                            "level": MODEL,
                            "state": FAILED,
                            "brand": model.get_brand(),
                            "model": model.get_name(),
                            "year": model.get_year(),
                            "url": model.get_url(),
                            "error": err.to_string(),
                            "retry_count": 0,
                        },
                        None,
                    )?;
                }
                LogLevel::Spec(spec) => {
                    self.0.insert_one(
                        doc! {
                            "level": SPEC,
                            "state": FAILED,
                            "brand": spec.get_brand(),
                            "model": spec.get_model(),
                            "year": spec.get_year(),
                            "error": err.to_string(),
                            "retry_count": 0,
                        },
                        None,
                    )?;
                }
            },
        }
        Ok(())
    }

    fn get_brand_errors(&self) -> Result<Vec<(Brand, String)>> {
        let docs = self.0.find(doc! {"level": { "$eq": BRAND }, "state": { "$eq": FAILED }, "retry_count": { "$lte": 3}}, None)?;
        let mut l = Vec::new();
        for doc in docs {
            let doc = doc?;
            let id = doc.get_str("_id")?;
            let brand = doc.get_str("brand")?;
            let url = doc.get_str("url")?;
            l.push((Brand::new(brand.to_owned(), url.to_owned()), id.to_owned()));
        }
        Ok(l)
    }

    fn get_model_errors(&self) -> Result<Vec<(Model, String)>> {
        let docs = self.0.find(doc! {"level": { "$eq": MODEL }, "state": { "$eq": FAILED }, "retry_count": { "$lte": 3}}, None)?;
        let mut l = Vec::new();
        for doc in docs {
            let doc = doc?;
            let id = doc.get_str("_id")?;
            let brand = doc.get_str("brand")?;
            let model = doc.get_str("model")?;
            let year = doc.get_str("year")?;
            let url = doc.get_str("url")?;
            l.push((Model::new(brand.to_owned(), model.to_owned(), year.to_owned(), url.to_owned()), id.to_owned()));
        }
        Ok(l)
    }

    fn get_spec_errors(&self) -> Result<Vec<(Spec, String)>> {
        let docs = self.0.find(doc! {"level": { "$eq": SPEC }, "state": { "$eq": FAILED }, "retry_count": { "$lte": 3}}, None)?;
        let mut l = Vec::new();
        for doc in docs {
            let doc = doc?;
            let id = doc.get_str("_id")?;
            let brand = doc.get_str("brand")?;
            let model = doc.get_str("model")?;
            let year = doc.get_str("year")?;
            l.push((Spec::new(brand.to_owned(), model.to_owned(), year.to_owned()), id.to_owned()));
        }
        Ok(l)
    }

    fn update_state(&mut self, id: &str, state: &str) -> Result<()> {
        self.0.update_one(
            doc! {
                "_id": id,
            },
            doc! {
                "$set": { "state": state },
            },
            None,
        )?;
        Ok(())
    }

    fn increment_retry_count(&mut self, id: &str) -> Result<()> {
        self.0.update_one(
            doc! {
                "_id": id,
            },
            doc! {
                "$inc": { "retry_count": 1},
            },
            None,
        )?;
        Ok(())
    }
}

mod test {

    #[test]
    fn db_test() {
        use crate::crawler::Store;
        use crate::db::MongoStore;
        use crate::result::Spec;

        let mut coll = MongoStore::new("motospec", "spec").unwrap();
        let mut spec = Spec::new("test".to_owned(), "test".to_owned(), "test".to_owned());
        spec.add_spec("a".to_owned(), "a".to_owned());
        coll.insert_spec(&spec).unwrap();
    }

    #[test]
    fn test_logger() {
        use super::MongoLog;
        use crate::crawler::Logger;
        let logger = MongoLog::new("motospec", "log").unwrap();
        let models = logger.get_model_errors().unwrap();
        for model in models {
            println!("{:?}", model.0);
        }
    }
}
