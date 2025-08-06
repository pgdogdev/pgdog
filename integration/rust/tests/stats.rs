#[derive(Debug)]
struct MetricNotFound;

impl std::fmt::Display for MetricNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetricNotFound")
    }
}

impl std::error::Error for MetricNotFound {
    fn description(&self) -> &str {
        "not found"
    }
}

pub async fn get_stat(name: &str) -> std::result::Result<i64, Box<dyn std::error::Error>> {
    let metrics = reqwest::get("http://127.0.0.1:9090/metrics").await.unwrap();

    for line in metrics.text().await.unwrap().split("\n") {
        if line.starts_with(name) {
            if let Some(val) = line.split(" ").last() {
                return Ok(val.parse()?);
            }
        }
    }

    Err(MetricNotFound.into())
}
