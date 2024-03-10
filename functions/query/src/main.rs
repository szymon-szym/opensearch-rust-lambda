use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};

use opensearch_service::{self, OpenSearchService, OpenSearchQueryBuilder};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Copy)]
struct Pagination {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Request {
    destination_city_name: Option<String>,
    origin_city_name: Option<String>,
    destination_weather: Option<String>,
    origin_weather: Option<String>,
    max_avg_ticket_price: Option<f64>,
    min_avg_ticket_price: Option<f64>,
    pagination: Option<Pagination>,
}

#[derive(Serialize)]
struct Response {
    flights: Vec<FlightData>,
}

async fn function_handler(os_client: &OpenSearchService, event: LambdaEvent<Request>) -> Result<Response, Error> {

    let request_body = event.payload;

    let index = "opensearch_dashboards_sample_data_flights";

    let limit = request_body.pagination.and_then(|p| p.limit).unwrap_or(10);

    let offset = request_body.pagination.and_then(|p| p.offset).unwrap_or(0);

    let dummy_query = OpenSearchQueryBuilder::new()
        .with_must_match("OriginWeather", request_body.origin_weather.unwrap_or("".to_string()))
        .with_must_match("DestWeather", request_body.destination_weather.unwrap_or("".to_string()))
        .with_must_match("DestCityName", request_body.destination_city_name.unwrap_or("".to_string()))
        .with_must_match("OriginCityName", request_body.origin_city_name.unwrap_or("".to_string()))
        .with_must_range("AvgTicketPrice", request_body.min_avg_ticket_price, request_body.max_avg_ticket_price)
        .build();

    let query_result = os_client.query::<FlightData>(index, limit, offset, dummy_query).await?;

    // Prepare the response
    let resp = Response {
        flights: query_result,
    };

    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    let os_client = opensearch_service::OpenSearchService::local_client();


    run(service_fn(|event: LambdaEvent<Request>| {
        function_handler(&os_client, event)
    })).await
}


#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct FlightData {
    flight_num: String,
    dest_country: String,
    origin_weather: String,
    origin_city_name: String,
    avg_ticket_price: f32,
    distance_miles: f32,
    flight_delay: bool,
    dest_weather: String,
    dest: String,
    flight_delay_type: String,
    origin_country: String,
    #[serde(rename = "dayOfWeek")]
    day_of_week: u8,
    distance_kilometers: f32,
    #[serde(rename = "timestamp")]
    timestamp: String,
    dest_location: Location,
    #[serde(rename = "DestAirportID")]
    dest_airport_id: String,
    carrier: String,
    cancelled: bool,
    flight_time_min: f32,
    origin: String,
    origin_location: Location,
    dest_region: String,
    #[serde(rename = "OriginAirportID")]
    origin_airport_id: String,
    origin_region: String,
    dest_city_name: String,
    flight_time_hour: f32,
    flight_delay_min: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Location {
    lat: String,
    lon: String,
}