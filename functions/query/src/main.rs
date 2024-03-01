use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};

use opensearch_service::{self, OpenSearchService};

use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Request {
    limit: i64,
}

#[derive(Serialize)]
struct Response {
    flights: Vec<FlightData>,
}

async fn function_handler(os_client: &OpenSearchService, event: LambdaEvent<Request>) -> Result<Response, Error> {

    let limit = event.payload.limit;

    let index = "opensearch_dashboards_sample_data_flights";

    let result = os_client.query_all_docs::<FlightData>(index, limit).await?;
    // Prepare the response
    let resp = Response {
        flights: result,
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