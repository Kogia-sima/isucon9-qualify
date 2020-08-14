use std::future::Future;

use actix_web::client::{Client, PayloadError, SendRequestError};
// use anyhow::Error;
use awc::http::StatusCode;
use bytes::Bytes;

const ISUCARI_API_TOKEN: &str = "Bearer 75ugk2m37a750fwir5xr-22l6h4wmue1bwrubzwd0";
const USER_AGENT: &str = "isucon9-qualify-webapp";

#[derive(Error, Debug)]
pub enum APIError {
    #[error("api request error: {0}")]
    RequestError(SendRequestError),
    #[error("api response error: {0}")]
    ResponseError(PayloadError),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Msg(String),
}

impl From<SendRequestError> for APIError {
    fn from(other: SendRequestError) -> Self {
        Self::RequestError(other)
    }
}

impl From<PayloadError> for APIError {
    fn from(other: PayloadError) -> Self {
        Self::ResponseError(other)
    }
}

#[derive(Serialize)]
pub struct PaymentServiceTokenReq {
    pub shop_id: String,
    pub token: String,
    pub api_key: String,
    pub price: i64,
}

#[derive(Deserialize)]
pub struct PaymentServiceTokenRes {
    pub status: String,
}

#[derive(Serialize)]
pub struct ShipmentCreateReq {
    pub to_address: String,
    pub to_name: String,
    pub from_address: String,
    pub from_name: String,
}

#[derive(Deserialize)]
pub struct ShipmentCreateRes {
    pub reserve_id: String,
    pub reserve_time: i64,
}

#[derive(Serialize)]
pub struct ShipmentRequestReq {
    pub reserve_id: String,
}

#[derive(Deserialize)]
pub struct ShipmentStatusRes {
    pub status: String,
    pub reserve_time: i64,
}

#[derive(Serialize)]
pub struct ShipmentStatusReq {
    pub reserve_id: String,
}

pub fn payment_token(
    payment_url: &str,
    param: &PaymentServiceTokenReq,
) -> impl Future<Output = Result<PaymentServiceTokenRes, APIError>> {
    let res = Client::new()
        .post(format!("{}/token", payment_url))
        .header("User-Agent", USER_AGENT)
        .send_json(param);

    async move {
        let mut res = res.await?;
        let status = res.status();
        let body = res.body().await?;
        if status != StatusCode::OK {
            return Err(APIError::Msg(format!(
                "status code: {}; body: {:?}",
                res.status(),
                &body[..]
            )));
        }

        serde_json::from_slice(body.as_ref()).map_err(APIError::Json)
    }
}

pub fn shipment_create(
    shipment_url: &str,
    param: &ShipmentCreateReq,
) -> impl Future<Output = Result<ShipmentCreateRes, APIError>> {
    let res = Client::new()
        .post(format!("{}/create", shipment_url))
        .header("User-Agent", USER_AGENT)
        .header("Authorization", ISUCARI_API_TOKEN)
        .send_json(param);

    async move {
        let mut res = res.await?;
        let status = res.status();
        let body = res.body().await?;
        if status != StatusCode::OK {
            return Err(APIError::Msg(format!(
                "status code: {}; body: {:?}",
                res.status(),
                &body[..]
            )));
        }

        serde_json::from_slice(body.as_ref()).map_err(APIError::Json)
    }
}

pub fn shipment_request(
    shipment_url: &str,
    param: &ShipmentRequestReq,
) -> impl Future<Output = Result<Bytes, APIError>> {
    let res = Client::new()
        .post(format!("{}/request", shipment_url))
        .header("User-Agent", USER_AGENT)
        .header("Authorization", ISUCARI_API_TOKEN)
        .send_json(param);

    async move {
        let mut res = res.await?;
        let status = res.status();
        let body = res.body().await?;
        if status != StatusCode::OK {
            return Err(APIError::Msg(format!(
                "status code: {}; body: {:?}",
                res.status(),
                &body[..]
            )));
        }

        Ok(body)
    }
}

pub fn shipment_status(
    shipment_url: &str,
    param: &ShipmentStatusReq,
) -> impl Future<Output = Result<ShipmentStatusRes, APIError>> {
    let res = Client::new()
        .post(format!("{}/status", shipment_url))
        .header("User-Agent", USER_AGENT)
        .header("Authorization", ISUCARI_API_TOKEN)
        .send_json(param);

    async move {
        let mut res = res.await?;
        let status = res.status();
        let body = res.body().await?;

        if status != StatusCode::OK {
            return Err(APIError::Msg(format!(
                "status code: {}; body: {}",
                status,
                String::from_utf8_lossy(body.as_ref())
            )));
        }

        serde_json::from_slice(body.as_ref()).map_err(APIError::Json)
    }
}
