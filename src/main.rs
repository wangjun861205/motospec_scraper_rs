extern crate async_recursion;
extern crate markup5ever;
extern crate mongodb;
extern crate reqwest;
extern crate scraper;
extern crate tokio;
extern crate url;

mod crawler;
mod db;
mod http;
mod result;

use crawler::{retry_scrape_models, retry_scrape_specs, scrape_brands, Logger};
use db::{MongoLog, MongoStore};
use http::HttpClient;
use result::Result;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let store = Arc::new(MongoStore::new("motospec", "spec")?);
    let logger = Arc::new(MongoLog::new("motospec", "log").unwrap());
    let client = Arc::new(HttpClient::new(128));
    let html = http::get(http::BASE_URL).await?;
    scrape_brands(client.clone(), &html, store.clone(), logger.clone()).await;
    let brands = logger.clone().get_brand_errors()?;
    for (brand, id) in brands {
        retry_scrape_models(client.clone(), brand, id, store.clone(), logger.clone()).await;
    }
    let models = logger.clone().get_model_errors()?;
    for (model, id) in models {
        retry_scrape_specs(client.clone(), model, id, store.clone(), logger.clone()).await;
    }
    Ok(())
}
