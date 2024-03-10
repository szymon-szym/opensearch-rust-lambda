use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use opensearch::{
    http::{
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    OpenSearch, SearchParts,
};

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
            .build()
            .unwrap();
        let client = OpenSearch::new(transport);

        Self { client }
    }

    pub async fn query<T>(
        &self,
        index: &str,
        limit: i64,
        offset: i64,
        query: OpenSearchQuery,
    ) -> anyhow::Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let query_json = json!(query);

        println!("query: {}", query_json);

        let response = self
            .client
            .search(SearchParts::Index(&[index]))
            .size(limit)
            .from(offset)
            .body(query_json)
            .send()
            .await?;

        let response_body = response.json::<Value>().await?;

        let result = response_body["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|raw_value| serde_json::from_value::<T>(raw_value["_source"].clone()).unwrap())
            .collect::<Vec<_>>();

        Ok(result)
    }

    // todo client for AWS
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OpenSearchQuery {
    query: BoolQuery,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BoolQuery {
    bool: MustQuery,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MustQuery {
    must: Vec<QueryStatement>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum QueryStatement {
    MatchStatement(MatchStatement),
    RangeStatement(RangeStatement),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MatchStatement {
    #[serde(rename = "match")]
    match_statement: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RangeStatement {
    range: Value,
}

pub struct OpenSearchQueryBuilder {
    query: OpenSearchQuery,
}

impl OpenSearchQueryBuilder {
    pub fn new() -> Self {
        Self {
            query: OpenSearchQuery {
                query: BoolQuery {
                    bool: MustQuery { must: vec![] },
                },
            },
        }
    }

    pub fn with_must_match(mut self, field: &str, value: String) -> Self {
        if value.is_empty() {
            return self;
        }
        self.query
            .query
            .bool
            .must
            .push(QueryStatement::MatchStatement(MatchStatement {
                match_statement: json!({
                    field: value
                }),
            }));
        self
    }

    pub fn with_must_range(mut self, field: &str, from: Option<f64>, to: Option<f64>) -> Self {
        let range = json!({
            field: {
                "gte": from,
                "lte": to
            }
        });

        self.query
            .query
            .bool
            .must
            .push(QueryStatement::RangeStatement(RangeStatement { range }));
        self
    }

    pub fn build(self) -> OpenSearchQuery {
        self.query
    }
}
