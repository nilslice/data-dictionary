use crate::db::Db;
use crate::dict::{Dataset, DatasetConfig, Manager};
use crate::error::Error as DDError;

use actix_http::Response;
use actix_web::{
    dev::HttpResponseBuilder,
    http::StatusCode,
    web::{Data, HttpRequest, Json, Path},
    Error, HttpResponse,
};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Clone)]
pub struct Server {
    pub db: Db,
}

#[derive(Deserialize)]
pub struct FindPartition {
    dataset_name: String,
    partition_name: String,
}

pub async fn find_partition(
    srv: Data<Server>,
    params: Path<FindPartition>,
) -> Result<HttpResponse, Error> {
    let mut resp = HttpResponse::build(StatusCode::OK);

    let dataset = Dataset::find(&mut srv.db.clone(), &params.dataset_name).await;
    if let Ok(dataset) = dataset {
        match dataset
            .partition(&mut srv.db.clone(), &params.partition_name)
            .await
        {
            Ok(partition) => resp.json(partition).await,
            Err(e) => match e {
                DDError::Sql(_) => {
                    json_message(
                        resp,
                        StatusCode::NOT_FOUND,
                        format!("no partition found with name '{}'", params.partition_name),
                    )
                    .await
                }
                _ => {
                    json_message(
                        resp,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("failed to find paritition '{}'", params.partition_name),
                    )
                    .await
                }
            },
        }
    } else {
        let msg = format!("no dataset found with name '{}'", params.dataset_name);
        let err = dataset.err().expect("no error found");
        log::error!("{}: {}", msg, err);

        match err {
            DDError::Sql(_) => json_message(resp, StatusCode::NOT_FOUND, msg).await,
            _ => {
                json_message(
                    resp,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to find dataset '{}'", params.dataset_name),
                )
                .await
            }
        }
    }
}

#[derive(Deserialize)]
pub struct LatestPartition {
    dataset_name: String,
}

pub async fn latest_partition(
    srv: Data<Server>,
    params: Path<LatestPartition>,
) -> Result<HttpResponse, Error> {
    let mut resp = HttpResponse::build(StatusCode::OK);
    let dataset = Dataset::find(&mut srv.db.clone(), &params.dataset_name).await;
    if let Ok(dataset) = dataset {
        match dataset.latest_partition(&mut srv.db.clone()).await {
            Ok(partition) => resp.json(partition).await,
            Err(e) => {
                log::error!(
                    "error finding latest partition for dataset '{}': {}",
                    params.dataset_name,
                    e
                );
                match e {
                    DDError::Sql(_) => {
                        json_message(
                            resp,
                            StatusCode::NOT_FOUND,
                            format!(
                                "no latest partition found for dataset '{}'",
                                params.dataset_name
                            ),
                        )
                        .await
                    }
                    _ => {
                        json_message(
                            resp,
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!(
                                "failed to find latest partition for dataset '{}'",
                                params.dataset_name
                            ),
                        )
                        .await
                    }
                }
            }
        }
    } else {
        let msg = format!(
            "failed to find dataset with name: '{}'",
            params.dataset_name
        );
        let err = dataset.err().expect("no error found");
        log::error!("{}: {}", msg, err);

        match err {
            DDError::Sql(_) => {
                json_message(
                    resp,
                    StatusCode::NOT_FOUND,
                    format!("no dataset found with name '{}'", params.dataset_name),
                )
                .await
            }
            _ => json_message(resp, StatusCode::INTERNAL_SERVER_ERROR, msg).await,
        }
    }
}

pub async fn register_dataset(
    srv: Data<Server>,
    config: Json<DatasetConfig>,
    req: HttpRequest,
) -> Result<HttpResponse, Error> {
    let invalid_api_key = |resp, req: HttpRequest| {
        log::error!(
            "failed to register dataset, invalid or missing API key, headers = {:?}",
            req.headers()
        );
        Ok(json_message(
            resp,
            StatusCode::UNAUTHORIZED,
            "invalid or missing API key",
        ))
    };

    let mut resp = HttpResponse::build(StatusCode::OK);

    let mut api_key = Uuid::default();
    if let Some(value) = req.headers().get("Authorization") {
        if let Ok(bearer) = value.to_str() {
            if let Ok(uuid) = Uuid::parse_str(&trim_api_key(bearer)) {
                api_key = uuid;
            } else {
                return invalid_api_key(resp, req);
            }
        }
    } else {
        return invalid_api_key(resp, req);
    }

    let manager = Manager::find(&mut srv.db.clone(), api_key).await;
    if let Ok(manager) = manager {
        match manager
            .register_dataset(
                &mut srv.db.clone(),
                &config.name,
                config.compression.clone(),
                config.encoding.clone(),
                config.classification.clone(),
                config.schema.to_owned(),
                &config.description,
            )
            .await
        {
            Ok(dataset) => resp.json(dataset).await,
            Err(e) => {
                log::error!(
                    "failed to register dataset '{}' from manager '{}': {}",
                    config.name,
                    api_key,
                    e
                );
                return Ok(json_message(
                    resp,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to register dataset '{}'", config.name),
                ));
            }
        }
    } else {
        let msg = format!("failed to find manager with API key '{}'", api_key);
        let err = manager.err().expect("no error found");
        log::error!("{}: {}", msg, err);

        match err {
            DDError::Sql(_) => {
                json_message(
                    resp,
                    StatusCode::NOT_FOUND,
                    format!("no manager found with API key '{}'", api_key),
                )
                .await
            }
            _ => json_message(resp, StatusCode::INTERNAL_SERVER_ERROR, msg).await,
        }
    }
}

pub async fn list_datasets(srv: Data<Server>) -> Result<HttpResponse, Error> {
    let mut resp = HttpResponse::build(StatusCode::OK);

    let datasets = Dataset::list(&mut srv.db.clone()).await;
    if let Ok(datasets) = datasets {
        resp.json(datasets).await
    } else {
        let msg = "failed to list datasets";
        let err = datasets.err().expect("no error found");
        log::error!("{}: {}", msg, err);

        match err {
            DDError::Sql(_) => json_message(resp, StatusCode::NOT_FOUND, "no datasets found").await,
            _ => json_message(resp, StatusCode::INTERNAL_SERVER_ERROR, msg).await,
        }
    }
}

fn json_message(
    mut builder: HttpResponseBuilder,
    status: StatusCode,
    message: impl AsRef<str>,
) -> Response {
    builder.status(status).json(serde_json::json!({
        "code": status.as_u16(),
        "status": status.canonical_reason(),
        "message": message.as_ref()
    }))
}

fn trim_api_key(bearer: &str) -> String {
    bearer.replace("Bearer ", "").trim().into()
}
