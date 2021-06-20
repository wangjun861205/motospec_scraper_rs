use crate::db::COMPLETED;
use crate::http::BASE_URL;
use crate::result::{Brand, Log, LogLevel, Model, Result, Spec};
use async_trait::async_trait;
use futures::future::join_all;
use futures::{future::BoxFuture, FutureExt};
use scraper::{ElementRef, Html, Selector};
use std::error::Error;
use std::sync::Arc;
use url::Url;

#[async_trait]
pub trait HttpGetter: Send + Sync {
    async fn get(&self, url: &str) -> Result<String>;
}

pub trait Store: Send + Sync {
    fn insert_spec(&self, spec: &Spec) -> Result<()>;
}

pub trait Logger: Send + Sync {
    fn insert_log(&self, log: Log<LogLevel, Box<dyn Error + Send + Sync>>) -> Result<()>;
    fn get_brand_errors(&self) -> Result<Vec<(Brand, String)>>;
    fn get_model_errors(&self) -> Result<Vec<(Model, String)>>;
    fn get_spec_errors(&self) -> Result<Vec<(Spec, String)>>;
    fn update_state(&self, id: &str, state: &str) -> Result<()>;
    fn increment_retry_count(&self, id: &str) -> Result<()>;
}

fn extract_brands(html: &str) -> Vec<Brand> {
    let root = Html::parse_document(html);
    let selector = Selector::parse("div[class=\"subMenu\"]>a[href*=\"/bikes/\"]").unwrap();
    root.select(&selector)
        .map(|node| {
            let brand_name = node.text().fold(String::new(), |mut s, v| {
                s.push_str(v);
                s
            });
            let href = Url::parse(BASE_URL).unwrap().join(node.value().attr(&"href").unwrap()).unwrap();
            Brand::new(brand_name, href.to_string())
        })
        .collect()
}

fn extract_models(html: &str, brand: &str) -> Vec<Model> {
    let root = Html::parse_document(html);
    let selector = Selector::parse("td a[href*=\"/model/\"]").unwrap();
    root.select(&selector)
        .map(|node| {
            let href = Url::parse(BASE_URL).unwrap().join(node.value().attr(&"href").unwrap()).unwrap();
            let model_name = node
                .text()
                .fold(String::new(), |mut s, v| {
                    s.push_str(v);
                    s
                })
                .replace("\n", " ")
                .trim()
                .to_owned();
            if let Some(tr) = node.ancestors().find(|v| {
                if let Some(ele) = v.value().as_element() {
                    return &ele.name.local == "tr";
                }
                return false;
            }) {
                if let Some(year_td) = tr
                    .children()
                    .filter(|v| {
                        if let Some(ele) = v.value().as_element() {
                            return &ele.name.local == "td";
                        }
                        false
                    })
                    .nth(1)
                {
                    if let Some(ele) = ElementRef::wrap(year_td) {
                        let year = ele
                            .text()
                            .fold(String::new(), |mut s, v| {
                                s.push_str(v);
                                s
                            })
                            .replace("\n", " ")
                            .trim()
                            .to_owned();

                        return Model::new(brand.to_owned(), model_name, year, href.to_string());
                    }
                }
            }
            Model::new(brand.to_owned(), model_name, "unknown".to_string(), href.to_string())
        })
        .collect()
}

fn extract_next_page(html: &str, brand: &str, prev_url: &str) -> Option<Brand> {
    let root = Html::parse_document(&html);
    let next_selector = Selector::parse("a").unwrap();
    if let Some(next) = root
        .select(&next_selector)
        .filter(|ele| {
            ele.text()
                .fold(String::new(), |mut s, v| {
                    s.push_str(v);
                    s
                })
                .trim()
                .to_owned()
                == "Next"
        })
        .nth(0)
    {
        if let Some(href) = next.value().attr("href") {
            let href = Url::parse(prev_url).unwrap().join(href).unwrap();
            return Some(Brand::new(brand.to_owned(), href.to_string()));
        }
    }
    None
}

fn extract_spec(html: &str, brand: &str, model: &str, year: &str) -> Spec {
    let root = Html::parse_document(html);
    let selector = Selector::parse("tr").unwrap();
    root.select(&selector)
        .filter(|ele| {
            ele.children()
                .filter(|c| {
                    if let Some(e) = c.value().as_element() {
                        return &e.name.local == "td";
                    }
                    false
                })
                .count()
                == 2
                && !ele.descendants().any(|c| {
                    if let Some(e) = c.value().as_element() {
                        return &e.name.local == "a";
                    }
                    false
                })
        })
        .fold(Spec::new(brand.to_owned(), model.to_owned(), year.to_owned()), |mut s, v| {
            let mut title = ElementRef::wrap(
                v.children()
                    .filter(|c| {
                        if let Some(e) = c.value().as_element() {
                            return &e.name.local == "td";
                        }
                        false
                    })
                    .nth(0)
                    .unwrap(),
            )
            .unwrap()
            .text()
            .fold(String::new(), |mut ss, vv| {
                ss.push_str(vv);
                ss
            });
            let mut value = ElementRef::wrap(
                v.children()
                    .filter(|c| {
                        if let Some(e) = c.value().as_element() {
                            return &e.name.local == "td";
                        }
                        false
                    })
                    .nth(1)
                    .unwrap(),
            )
            .unwrap()
            .text()
            .fold(String::new(), |mut ss, vv| {
                ss.push_str(vv);
                ss
            });
            title = title.trim().to_owned();
            value = value.trim().to_owned();
            if title != "" && value != "" {
                s.add_spec(title, value);
            }
            s
        })
}

pub async fn scrape_brands(getter: Arc<dyn HttpGetter>, html: &str, store: Arc<dyn Store>, logger: Arc<dyn Logger>) {
    let brands = extract_brands(html);
    // let mut handles = Vec::new();
    for brand in brands {
        // handles.push(tokio::spawn(scrape_models(getter.clone(), brand, store.clone(), logger.clone())));
        scrape_models(getter.clone(), brand, store.clone(), logger.clone()).await;
    }
    // join_all(handles).await;
}

fn scrape_models<'a>(getter: Arc<dyn HttpGetter>, brand: Brand, store: Arc<dyn Store>, logger: Arc<dyn Logger>) -> BoxFuture<'a, ()> {
    async move {
        match getter.get(brand.get_url()).await {
            Ok(html) => {
                let mut handles = Vec::new();
                let models = extract_models(&html, brand.get_name());
                for model in models {
                    handles.push(tokio::spawn(scrape_specs(getter.clone(), model, store.clone(), logger.clone())));
                }
                if let Some(next) = extract_next_page(&html, brand.get_name(), brand.get_url()) {
                    handles.push(tokio::spawn(scrape_models(getter.clone(), next, store.clone(), logger.clone())));
                }
                join_all(handles).await;
                logger.insert_log(Log::Log(LogLevel::Brand(brand))).unwrap();
            }
            Err(e) => logger.insert_log(Log::Err(LogLevel::Brand(brand), e)).unwrap(),
        }
    }
    .boxed()
}

pub fn retry_scrape_models<'a>(getter: Arc<dyn HttpGetter>, brand: Brand, log_id: String, store: Arc<dyn Store>, logger: Arc<dyn Logger>) -> BoxFuture<'a, ()> {
    async move {
        match getter.get(brand.get_url()).await {
            Ok(html) => {
                let mut handles = Vec::new();
                let models = extract_models(&html, brand.get_name());
                for model in models {
                    handles.push(tokio::spawn(scrape_specs(getter.clone(), model, store.clone(), logger.clone())));
                }
                if let Some(next) = extract_next_page(&html, brand.get_name(), brand.get_url()) {
                    handles.push(tokio::spawn(scrape_models(getter.clone(), next, store.clone(), logger.clone())));
                }
                join_all(handles).await;
                logger.update_state(&log_id, COMPLETED).unwrap();
            }
            Err(_) => logger.increment_retry_count(&log_id).unwrap(),
        }
    }
    .boxed()
}

async fn scrape_specs(getter: Arc<dyn HttpGetter>, model: Model, store: Arc<dyn Store>, logger: Arc<dyn Logger>) {
    match getter.get(model.get_url()).await {
        Ok(html) => {
            let mut spec = extract_spec(&html, model.get_brand(), model.get_name(), model.get_year());
            spec.add_spec("Brand".to_owned(), model.get_brand().to_owned());
            spec.add_spec("Model".to_owned(), model.get_name().to_owned());
            if let Err(e) = store.insert_spec(&spec) {
                logger.insert_log(Log::Err(LogLevel::Spec(spec), e)).unwrap();
            } else {
                logger.insert_log(Log::Log(LogLevel::Spec(spec))).unwrap();
            }
        }
        Err(e) => logger.insert_log(Log::Err(LogLevel::Model(model), e)).unwrap(),
    }
}

pub async fn retry_scrape_specs(getter: Arc<dyn HttpGetter>, model: Model, log_id: String, store: Arc<dyn Store>, logger: Arc<dyn Logger>) {
    match getter.get(model.get_url()).await {
        Ok(html) => {
            let mut spec = extract_spec(&html, model.get_brand(), model.get_name(), model.get_year());
            spec.add_spec("Brand".to_owned(), model.get_brand().to_owned());
            spec.add_spec("Model".to_owned(), model.get_name().to_owned());
            if let Err(_) = store.insert_spec(&spec) {
                logger.increment_retry_count(&log_id).unwrap();
            } else {
                logger.update_state(&log_id, COMPLETED).unwrap();
            }
        }
        Err(_) => logger.increment_retry_count(&log_id).unwrap(),
    }
}

#[cfg(test)]
mod test {
    use tokio::runtime::Runtime;

    use super::scrape_brands;
    use super::{extract_brands, extract_next_page, extract_spec};
    use crate::db::{MongoLog, MongoStore};
    use crate::http::{self, HttpClient};
    use std::sync::Arc;

    #[test]
    fn test_scrape() {
        let rt = Runtime::new().unwrap();
        let html = rt.block_on(http::get(http::BASE_URL)).unwrap();
        let store = Arc::new(MongoStore::new("motospec", "spec").unwrap());
        let logger = Arc::new(MongoLog::new("motospec", "log").unwrap());
        let client = Arc::new(HttpClient::new(32));
        rt.block_on(scrape_brands(client, &html, store, logger));
    }

    #[test]
    fn test_next_page() {
        let rt = Runtime::new().unwrap();
        let html = rt.block_on(http::get("https://www.motorcyclespecs.co.za/bikes/mv_agusta.html")).unwrap();
        println!("{:?}", extract_next_page(&html, "MV", "https://www.motorcyclespecs.co.za/bikes/mv_agusta.html"));
    }

    #[test]
    fn test_extract_brands() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let html = rt.block_on(http::get(http::BASE_URL)).unwrap();
        for brand in extract_brands(&html) {
            println!("{:?}", brand);
        }
    }

    #[test]
    fn test_extract_spec() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let html = rt.block_on(http::get("https://www.motorcyclespecs.co.za/model/Honda/honda_adv150.html")).unwrap();
        let spec = extract_spec(&html, "honda", "xadv150", "2021");
        println!("{:?}", spec);
    }
}
