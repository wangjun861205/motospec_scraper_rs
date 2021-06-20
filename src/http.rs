use crate::{crawler::HttpGetter, result::Result};
use async_trait::async_trait;
use reqwest::{
    header::{HeaderName, HeaderValue},
    Client, Method,
};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Semaphore;

static DEFAULT_HEADERS: &'static [(&'static str, &'static str)] = &[
    (
        "Accept",
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    ),
    ("Accept-Encoding", "gzip, deflate, br"),
    ("Accept-Language", "en,zh-CN;q=0.9,zh;q=0.8,de;q=0.7"),
    ("Cache-Control", "no-cache"),
    ("Connection", "keep-alive"),
    (
        "Cookie",
        "_ga=GA1.3.1854899192.1618532505; _gid=GA1.3.1610962403.1618532505; __gads=ID=27e2c4015d0dff8a-22d058a6a9b90026:T=1618532505:RT=1618532505:S=ALNI_MaixyAR9NetpX_DMq6M9VZ-G9Gn0A",
    ),
    ("Host", "www.motorcyclespecs.co.za"),
    ("Pragma", "no-cache"),
    // ("Referer", "https://www.motorcyclespecs.co.za/index.htm"),
    ("sec-ch-ua", "\"Google Chrome\";v=\"89\", \"Chromium\";v=\"89\", \";Not A Brand\";v=\"99\""),
    ("sec-ch-ua-mobile", "?0"),
    ("Sec-Fetch-Dest", "document"),
    ("Sec-Fetch-Mode", "navigate"),
    ("Sec-Fetch-Site", "same-origin"),
    ("Sec-Fetch-User", "?1"),
    ("Upgrade-Insecure-Requests", "1"),
    (
        "User-Agent",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36",
    ),
];

pub static BASE_URL: &'static str = "https://www.motorcyclespecs.co.za/index.htm";

#[derive(Debug, Clone)]
pub struct HttpClient {
    client: Client,
    semaphore: Arc<Semaphore>,
}

impl HttpClient {
    pub fn new(num_conns: usize) -> Self {
        Self {
            client: Client::new(),
            semaphore: Arc::new(Semaphore::new(num_conns)),
        }
    }
}

#[async_trait]
impl HttpGetter for HttpClient {
    async fn get(&self, url: &str) -> Result<String> {
        let sem = self.semaphore.acquire().await?;
        let req = self
            .client
            .request(Method::GET, url)
            .headers(DEFAULT_HEADERS.iter().map(|(k, v)| (HeaderName::from_str(k).unwrap(), HeaderValue::from_str(v).unwrap())).collect())
            .build()?;
        let res = self.client.execute(req).await?;
        drop(sem);
        res.text().await.map_err(|e| e.into())
    }
}

pub async fn get(url: &str) -> Result<String> {
    let client = Client::new();
    let req = client
        .request(Method::GET, url)
        .headers(DEFAULT_HEADERS.iter().map(|(k, v)| (HeaderName::from_str(k).unwrap(), HeaderValue::from_str(v).unwrap())).collect())
        .build()?;
    Ok(client.execute(req).await?.text().await?)
}
