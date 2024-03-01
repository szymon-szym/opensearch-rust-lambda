use serde_json::{json, Value};
use serde::de::DeserializeOwned;

use opensearch::{http::{transport::{SingleNodeConnectionPool, TransportBuilder}, Url}, OpenSearch, SearchParts};

pub struct OpenSearchService {
    client: OpenSearch,    
}

impl OpenSearchService {

    pub fn local_client() -> Self {
        let url = Url::parse("https://localhost:9200").unwrap();
        let conn_pool = SingleNodeConnectionPool::new(url);
        let credentials =
            opensearch::auth::Credentials::Basic("admin".to_string(), "admin".to_string());
        let cert_validation = opensearch::cert::CertificateValidation::None;
        let transport = TransportBuilder::new(conn_pool)
            .cert_validation(cert_validation)
            .auth(credentials)
            .build().unwrap();
        let client = OpenSearch::new(transport);

        Self { client }
    }

    pub async fn query_all_docs<T>(&self, index: &str, limit: i64) -> anyhow::Result<Vec<T>> 
    where T: DeserializeOwned
    {
        let response = self.client
            .search(SearchParts::Index(&[index]))
            .size(limit)
            .from(0)
            .body(json!({
                "query": {
                    "bool": {
                        "must": []
                    }
                }
            }))
            .send()
            .await?;

        let response_body = response.json::<Value>().await?;

        let result = response_body["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|raw_value| {
                serde_json::from_value::<T>(raw_value["_source"].clone()).unwrap()
            })
            .collect::<Vec<_>>();

        Ok(result)
    }

    // todo client for AWS
}

