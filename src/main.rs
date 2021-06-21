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
use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let mongo_uri = env::var("MONGO_URI")?;
    let mongo_db = env::var("MONGO_DB")?;
    let mongo_data_coll = env::var("MONGO_DATA_COLL")?;
    let mongo_log_coll = env::var("MONGO_LOG_COLL")?;
    let num_of_http_conn = env::var("NUM_OF_HTTP_CONN")?.parse::<usize>()?;
    let store = Arc::new(MongoStore::new(&mongo_uri, &mongo_db, &mongo_data_coll)?);
    let logger = Arc::new(MongoLog::new(&mongo_uri, &mongo_db, &mongo_log_coll)?);
    let client = Arc::new(HttpClient::new(num_of_http_conn).unwrap());
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
