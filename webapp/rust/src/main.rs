#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate thiserror;
#[macro_use]
extern crate actix_web_codegen;

mod api;

use actix_files::Files;
use actix_session::{CookieSession, Session};
use actix_web::dev::Body;
use actix_web::error::{
    ErrorBadRequest, ErrorForbidden, ErrorInternalServerError, ErrorNotFound, ErrorUnauthorized,
    ErrorUnprocessableEntity,
};
use actix_web::http::StatusCode;
use actix_web::middleware::Logger;
use actix_web::{web, App, FromRequest, HttpRequest, HttpResponse, HttpServer, ResponseError};
use chrono::naive::NaiveDateTime;
use chrono::Duration;
use mysql_async::prelude::*;
use mysql_async::{Conn, FromRowError, Row};
use rand::{rngs::SmallRng, RngCore, SeedableRng};
use std::env;
use std::fmt;
use std::fs;
use std::future::Future;
use std::io::{Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::pin::Pin;
use std::process::Command;

const SESSION_NAME: &str = "session_isucari";

const DEFAULT_PAYMENT_SERVICE_URL: &str = "http://localhost:5555";
const DEFAULT_SHIPMENT_SERVICE_URL: &str = "http://localhost:7000";

const ITEM_MIN_PRICE: i64 = 100;
const ITEM_MAX_PRICE: i64 = 1000000;
const ITEM_PRICE_ERR_MSG: &str = "商品価格は100ｲｽｺｲﾝ以上、1,000,000ｲｽｺｲﾝ以下にしてください";

const ITEM_STATUS_ON_SALE: &str = "on_sale";
const ITEM_STATUS_TRADING: &str = "trading";
const ITEM_STATUS_SOLD_OUT: &str = "sold_out";
const ITEM_STATUS_STOP: &str = "stop";
const ITEM_STATUS_CANCEL: &str = "cancel";

const PAYMENT_SERVICE_ISUCARI_APIKEY: &str = "a15400e46c83635eb181-946abb51ff26a868317c";
const PAYMENT_SERVICE_ISUCARI_SHOP_ID: &str = "11";

const TRANSACTION_EVIDENCE_STATUS_WAIT_SHIPPING: &str = "wait_shipping";
const TRANSACTION_EVIDENCE_STATUS_WAIT_DONE: &str = "wait_done";
const TRANSACTION_EVIDENCE_STATUS_DONE: &str = "done";

const SHIPPINGS_STATUS_INITIAL: &str = "initial";
const SHIPPINGS_STATUS_WAIT_PICKUP: &str = "wait_pickup";
const SHIPPINGS_STATUS_SHIPPING: &str = "shipping";
const SHIPPINGS_STATUS_DONE: &str = "done";

const BUMP_CHARGE_SECONDS: i64 = 3;

const ITEMS_PER_PAGE: i64 = 48;
const TRANSACTIONS_PER_PAGE: u32 = 10;

macro_rules! try_or_rollback {
    ($e:expr, $tx:ident) => {
        match $e {
            Ok(val) => val,
            Err(e) => {
                $tx.rollback().await?;
                return Err(e.into());
            }
        }
    };
}

#[derive(Debug, Error)]
enum Error {
    #[error(transparent)]
    Actix(#[from] actix_web::Error),
    #[error("db error")]
    DbError(#[from] mysql_async::Error),
}

impl ResponseError for Error {
    fn status_code(&self) -> StatusCode {
        match *self {
            Error::Actix(ref e) => e.as_response_error().status_code(),
            Error::DbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse<Body> {
        HttpResponse::build(self.status_code())
            .content_type("application/json")
            .body(format!(r#"{{"error":{:?}}}"#, self.to_string()))
    }
}

/// shared data
struct SharedData {
    db: mysql_async::Pool,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserSession {
    user_id: i64,
    csrf_token: String,
}

#[derive(Serialize)]
struct Config {
    name: String,
    val: String,
}

impl FromRow for Config {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError> {
        FromRow::from_row_opt(row).map(|(name, val)| Config { name, val })
    }
}

#[derive(Debug, Serialize)]
struct User {
    id: i64,
    account_name: String,
    #[serde(skip)]
    hashed_password: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    address: String,
    num_sell_items: i64,
    #[serde(skip)]
    last_bump: NaiveDateTime,
    #[serde(skip)]
    #[allow(dead_code)]
    created_at: NaiveDateTime,
}

impl FromRow for User {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError> {
        FromRow::from_row_opt(row).map(
            |(
                id,
                account_name,
                hashed_password,
                address,
                num_sell_items,
                last_bump,
                created_at,
            )| {
                User {
                    id,
                    account_name,
                    hashed_password,
                    address,
                    num_sell_items,
                    last_bump,
                    created_at,
                }
            },
        )
    }
}

#[derive(Debug, Serialize, Clone)]
struct UserSimple {
    id: i64,
    account_name: String,
    num_sell_items: i64,
}

impl FromRow for UserSimple {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError> {
        FromRow::from_row_opt(row).map(|(id, account_name, num_sell_items)| UserSimple {
            id,
            account_name,
            num_sell_items,
        })
    }
}

#[derive(Serialize)]
struct Item {
    id: i64,
    seller_id: i64,
    buyer_id: i64,
    status: String,
    name: String,
    price: i64,
    description: String,
    image_name: String,
    category_id: i64,
    #[serde(skip)]
    created_at: NaiveDateTime,
    #[serde(skip)]
    updated_at: NaiveDateTime,
}

impl fmt::Debug for Item {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Item")
            .field("id", &self.id)
            .field("seller_id", &self.seller_id)
            .field("buyer_id", &self.buyer_id)
            .field("status", &self.status)
            .field("category_id", &self.category_id)
            .finish()
    }
}

impl FromRow for Item {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError> {
        FromRow::from_row_opt(row).map(
            |(
                id,
                seller_id,
                buyer_id,
                status,
                name,
                price,
                description,
                image_name,
                category_id,
                created_at,
                updated_at,
            )| Item {
                id,
                seller_id,
                buyer_id,
                status,
                name,
                price,
                description,
                image_name,
                category_id,
                created_at,
                updated_at,
            },
        )
    }
}

#[derive(Serialize)]
struct ItemSimple {
    id: i64,
    seller_id: i64,
    seller: UserSimple,
    status: String,
    name: String,
    price: i64,
    image_url: String,
    category_id: i64,
    category: Category,
    created_at: i64,
}

impl fmt::Debug for ItemSimple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Item")
            .field("id", &self.id)
            .field("seller_id", &self.seller_id)
            .field("seller", &self.seller)
            .field("status", &self.status)
            .field("category_id", &self.category_id)
            .field("category", &self.category)
            .finish()
    }
}

#[derive(Serialize)]
struct ItemDetail {
    id: i64,
    seller_id: i64,
    seller: UserSimple,
    #[serde(skip_serializing_if = "Option::is_none")]
    buyer_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    buyer: Option<UserSimple>,
    status: String,
    name: String,
    price: i64,
    description: String,
    image_url: String,
    category_id: i64,
    category: Category,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_evidence_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_evidence_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shipping_status: Option<String>,
    created_at: i64,
}

#[derive(Serialize)]
struct TransactionEvidence {
    id: i64,
    seller_id: i64,
    buyer_id: i64,
    status: String,
    item_id: i64,
    item_name: String,
    item_price: i64,
    item_description: String,
    item_category_id: i64,
    item_root_category_id: i64,
    #[serde(skip)]
    #[allow(dead_code)]
    created_at: NaiveDateTime,
    #[serde(skip)]
    #[allow(dead_code)]
    updated_at: NaiveDateTime,
}

impl FromRow for TransactionEvidence {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError> {
        FromRow::from_row_opt(row).map(
            |(
                id,
                seller_id,
                buyer_id,
                status,
                item_id,
                item_name,
                item_price,
                item_description,
                item_category_id,
                item_root_category_id,
                created_at,
                updated_at,
            )| TransactionEvidence {
                id,
                seller_id,
                buyer_id,
                status,
                item_id,
                item_name,
                item_price,
                item_description,
                item_category_id,
                item_root_category_id,
                created_at,
                updated_at,
            },
        )
    }
}

#[derive(Serialize)]
struct Shipping {
    transaction_evidence_id: i64,
    status: String,
    item_name: String,
    item_id: i64,
    reserve_id: String,
    reserve_time: i64,
    to_address: String,
    to_name: String,
    from_address: String,
    from_name: String,
    #[serde(skip)]
    img_binary: Vec<u8>,
    #[serde(skip)]
    #[allow(dead_code)]
    created_at: NaiveDateTime,
    #[serde(skip)]
    #[allow(dead_code)]
    updated_at: NaiveDateTime,
}

impl FromRow for Shipping {
    fn from_row_opt(mut row: Row) -> Result<Self, FromRowError> {
        if row.len() != 13 {
            return Err(FromRowError(row));
        }

        let mut inner = || -> Result<Self, mysql_async::FromValueError> {
            Ok(Shipping {
                transaction_evidence_id: row.take_opt(0).unwrap()?,
                status: row.take_opt(1).unwrap()?,
                item_name: row.take_opt(2).unwrap()?,
                item_id: row.take_opt(3).unwrap()?,
                reserve_id: row.take_opt(4).unwrap()?,
                reserve_time: row.take_opt(5).unwrap()?,
                to_address: row.take_opt(6).unwrap()?,
                to_name: row.take_opt(7).unwrap()?,
                from_address: row.take_opt(8).unwrap()?,
                from_name: row.take_opt(9).unwrap()?,
                img_binary: row.take_opt(10).unwrap()?,
                created_at: row.take_opt(11).unwrap()?,
                updated_at: row.take_opt(12).unwrap()?,
            })
        };

        inner().map_err(|_| FromRowError(row))
    }
}

#[derive(Debug, Serialize)]
struct Category {
    id: i64,
    parent_id: i64,
    category_name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    parent_category_name: String,
}

impl FromRow for Category {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError> {
        FromRow::from_row_opt(row).map(|(id, parent_id, category_name)| Category {
            id,
            parent_id,
            category_name,
            parent_category_name: String::new(),
        })
    }
}

#[derive(Debug, Deserialize)]
struct ReqInitialize {
    payment_service_url: String,
    shipment_service_url: String,
}

#[derive(Serialize)]
struct ResInitialize {
    campaign: i64,
    language: String,
}

#[derive(Serialize)]
struct ResNewItems {
    #[serde(skip_serializing_if = "Option::is_none")]
    root_category_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    root_category_name: Option<String>,
    has_next: bool,
    items: Vec<ItemSimple>,
}

#[derive(Serialize)]
struct ResUserItems {
    user: UserSimple,
    has_next: bool,
    items: Vec<ItemSimple>,
}

#[derive(Serialize)]
struct ResTransactions {
    has_next: bool,
    items: Vec<ItemDetail>,
}

#[derive(Debug, Deserialize)]
struct ReqRegister {
    account_name: String,
    address: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct ReqLogin {
    account_name: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct ReqItemEdit {
    csrf_token: String,
    item_id: i64,
    item_price: i64,
}

#[derive(Serialize)]
struct ResItemEdit {
    item_id: i64,
    item_price: i64,
    item_created_at: i64,
    item_updated_at: i64,
}

#[derive(Debug, Deserialize)]
struct ReqBuy {
    csrf_token: String,
    item_id: i64,
    token: String,
}

#[derive(Serialize)]
struct ResBuy {
    transaction_evidence_id: i64,
}

#[derive(Serialize)]
struct ResSell {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct ReqPostShip {
    csrf_token: String,
    item_id: i64,
}

#[derive(Serialize)]
struct ResPostShip {
    path: String,
    reserve_id: String,
}

#[derive(Debug, Deserialize)]
struct ReqPostShipDone {
    csrf_token: String,
    item_id: i64,
}

#[derive(Debug, Deserialize)]
struct ReqPostComplete {
    csrf_token: String,
    item_id: i64,
}

#[derive(Debug, Deserialize)]
struct ReqBump {
    csrf_token: String,
    item_id: i64,
}

#[derive(Serialize)]
struct ResSetting {
    csrf_token: String,
    payment_service_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<User>,
    categories: Vec<Category>,
}

#[derive(Debug, Deserialize)]
struct Query {
    item_id: Option<i64>,
    created_at: Option<i64>,
}

impl FromRequest for Query {
    type Error = actix_web::Error;
    type Future = futures::future::Ready<Result<Query, actix_web::Error>>;
    type Config = ();

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let inner = web::Query::<Query>::from_request(req, payload)
            .into_inner()
            .and_then(|query| {
                let query = query.into_inner();
                if matches!(query.item_id, Some(i64::MIN..=0)) {
                    Err(ErrorBadRequest("item_id param error"))
                } else if matches!(query.created_at, Some(i64::MIN..=0)) {
                    Err(ErrorBadRequest("created_at param error"))
                } else {
                    Ok(query)
                }
            });
        futures::future::ready(inner)
    }
}

fn exec_first_or<'a: 'b, 'b, T: Queryable, S: ?Sized, P, E, R>(
    conn: &'a mut T,
    stmt: &'b S,
    params: P,
    err: E,
) -> impl Future<Output = Result<R, Error>> + 'b
where
    T: Queryable,
    S: StatementLike + 'b,
    P: Into<mysql_async::Params> + Send + 'b,
    E: Into<Error> + 'static,
    R: FromRow + Send + 'static,
{
    async move {
        let opt = conn.exec_first(stmt, params).await?;
        opt.ok_or(err.into())
    }
}

fn get_image_url(image_name: &str) -> String {
    format!("/upload/{}", image_name)
}

fn get_csrf_token(session: &Session) -> Result<String, actix_web::Error> {
    match session.get(SESSION_NAME) {
        Ok(Some(UserSession { csrf_token, .. })) => Ok(csrf_token),
        Ok(None) => Ok(String::new()),
        Err(e) => Err(e),
    }
}

async fn get_user(conn: &mut Conn, session: &Session) -> Result<User, Error> {
    let user_id = match session.get(SESSION_NAME) {
        Ok(Some(UserSession { user_id, .. })) => user_id,
        _ => return Err(ErrorNotFound("no session").into()),
    };

    let user = conn
        .exec_first("SELECT * FROM `users` WHERE `id` = ?", (user_id,))
        .await?;

    user.ok_or_else(|| Error::from(ErrorNotFound("user not found")))
}

async fn get_user_simple_by_id<T: Queryable>(
    conn: &mut T,
    user_id: i64,
) -> Result<UserSimple, Error> {
    let user = conn
        .exec_first(
            "SELECT `id`, `account_name`, `num_sell_items` FROM `users` WHERE `id` = ?",
            (user_id,),
        )
        .await?;

    user.ok_or_else(|| ErrorNotFound("user not found").into())
}

fn get_category_by_id<'a, T: Queryable>(
    conn: &'a mut T,
    category_id: i64,
) -> Pin<Box<dyn Future<Output = Result<Category, Error>> + 'a>> {
    Box::pin(async move {
        let category = conn
            .exec_first("SELECT * FROM `categories` WHERE `id` = ?", (category_id,))
            .await?;

        let mut category: Category = category.ok_or_else(|| ErrorNotFound("category not found"))?;

        if category.parent_id != 0 {
            let parent_category = get_category_by_id(conn, category.parent_id).await?;
            category.parent_category_name = parent_category.category_name;
        }

        Ok(category)
    })
}

async fn get_config_by_name<T: Queryable>(conn: &mut T, name: &str) -> Result<String, Error> {
    let config: Option<Config> = conn
        .exec_first("SELECT * FROM `configs` WHERE `name` = ?", (name,))
        .await?;
    match config {
        Some(config) => Ok(config.val),
        None => Err(ErrorNotFound("config not found").into()),
    }
}

async fn get_payment_service_url<T: Queryable>(conn: &mut T) -> String {
    get_config_by_name(conn, "payment_service_url")
        .await
        .unwrap_or_else(|_| DEFAULT_PAYMENT_SERVICE_URL.to_string())
}

async fn get_shipment_service_url<T: Queryable>(conn: &mut T) -> String {
    get_config_by_name(conn, "shipment_service_url")
        .await
        .unwrap_or_else(|_| DEFAULT_SHIPMENT_SERVICE_URL.to_string())
}

async fn get_index() -> HttpResponse {
    HttpResponse::build(StatusCode::OK)
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../../public/index.html"))
}

#[post("/initialize")]
async fn post_initialize(
    body: web::Json<ReqInitialize>,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResInitialize>, Error> {
    let status = Command::new("bash").arg("../sql/init.sh").status();
    if !matches!(status, Ok(status) if status.success()) {
        return Err(ErrorInternalServerError("exec init.sh error").into());
    }

    let mut conn = data.db.get_conn().await?;
    conn.exec_drop(
 		"INSERT INTO `configs` (`name`, `val`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `val` = VALUES(`val`)",
        ("payment_service_url", &body.payment_service_url)
    ).await?;

    conn.exec_drop(
 		"INSERT INTO `configs` (`name`, `val`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `val` = VALUES(`val`)",
        ("shipment_service_url", &body.shipment_service_url)
    ).await?;

    Ok(web::Json(ResInitialize {
        campaign: 0,
        language: "rust".to_string(),
    }))
}

#[get("/new_items.json")]
async fn get_new_items(
    query: Query,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResNewItems>, Error> {
    let mut conn = data.db.get_conn().await?;

    let fut = if let Query {
        item_id: Some(item_id),
        created_at: Some(created_at),
    } = query
    {
        conn.exec(
            "SELECT * FROM `items` WHERE `status` IN (?,?) AND (`created_at` < ?  OR (`created_at` <= ? AND `id` < ?)) ORDER BY `created_at` DESC, `id` DESC LIMIT ?",
            (
                ITEM_STATUS_ON_SALE,
                ITEM_STATUS_SOLD_OUT,
                NaiveDateTime::from_timestamp(created_at, 0),
                NaiveDateTime::from_timestamp(created_at, 0),
                item_id,
                ITEMS_PER_PAGE + 1
            )
        )
    } else {
        conn.exec(
 			"SELECT * FROM `items` WHERE `status` IN (?,?) ORDER BY `created_at` DESC, `id` DESC LIMIT ?",
            (
                ITEM_STATUS_ON_SALE,
                ITEM_STATUS_SOLD_OUT,
                ITEMS_PER_PAGE + 1
            )
 		)
    };

    let items: Vec<Item> = fut.await?;
    let mut item_simples = Vec::<ItemSimple>::with_capacity(items.len());

    for item in items {
        let seller = get_user_simple_by_id(&mut conn, item.seller_id).await?;
        let category = get_category_by_id(&mut conn, item.category_id).await?;
        item_simples.push(ItemSimple {
            id: item.id,
            seller_id: item.seller_id,
            seller,
            status: item.status,
            name: item.name,
            price: item.price,
            image_url: get_image_url(&item.image_name),
            category_id: item.category_id,
            category,
            created_at: item.created_at.timestamp(),
        })
    }

    let mut has_next = false;
    if item_simples.len() > ITEMS_PER_PAGE as usize {
        has_next = true;
        item_simples.truncate(ITEMS_PER_PAGE as usize);
    }

    Ok(web::Json(ResNewItems {
        items: item_simples,
        has_next,
        root_category_id: None,
        root_category_name: None,
    }))
}

#[get("/new_items/{root_category_id}.json")]
async fn get_new_category_items(
    root_category_id: web::Path<i64>,
    query: Query,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResNewItems>, Error> {
    let root_category_id = root_category_id.into_inner();
    if root_category_id <= 0 {
        return Err(ErrorBadRequest("incorrect category id").into());
    }

    let mut conn = data.db.get_conn().await?;
    let root_category = get_category_by_id(&mut conn, root_category_id)
        .await
        .map_err(|_| ErrorNotFound("category not found".to_string()))?;

    let category_ids: Vec<i64> = conn
        .exec(
            "SELECT id FROM `categories` WHERE parent_id=?",
            (root_category.id,),
        )
        .await?;

    let mut categories_ids_str = String::with_capacity(category_ids.len() * 2);
    for id in &category_ids {
        use std::fmt::Write;

        let _ = write!(categories_ids_str, "{},", id);
    }
    if !categories_ids_str.is_empty() {
        categories_ids_str.truncate(categories_ids_str.len() - 1);
    }

    let items: Vec<Item> = if let Query {
        item_id: Some(item_id),
        created_at: Some(created_at),
    } = query
    {
        // integers do not need sanitizing
        let sql = format!("SELECT * FROM `items` WHERE `status` IN (?,?) AND category_id IN ({}) AND (`created_at` < ?  OR (`created_at` <= ? AND `id` < ?)) ORDER BY `created_at` DESC, `id` DESC LIMIT ?", categories_ids_str);
        // paging
        conn.exec(
            &*sql,
            (
                ITEM_STATUS_ON_SALE,
                ITEM_STATUS_SOLD_OUT,
                NaiveDateTime::from_timestamp(created_at, 0),
                NaiveDateTime::from_timestamp(created_at, 0),
                item_id,
                ITEMS_PER_PAGE + 1,
            ),
        )
        .await?
    } else {
        let sql = format!("SELECT * FROM `items` WHERE `status` IN (?,?) AND category_id IN ({}) ORDER BY created_at DESC, id DESC LIMIT ?", categories_ids_str);
        conn.exec(
            &*sql,
            (
                ITEM_STATUS_ON_SALE,
                ITEM_STATUS_SOLD_OUT,
                ITEMS_PER_PAGE + 1,
            ),
        )
        .await?
    };

    let mut item_simples = Vec::<ItemSimple>::with_capacity(items.len());

    for item in items {
        let seller = get_user_simple_by_id(&mut conn, item.seller_id).await?;
        let category = get_category_by_id(&mut conn, item.category_id).await?;
        item_simples.push(ItemSimple {
            id: item.id,
            seller_id: item.seller_id,
            seller,
            status: item.status,
            name: item.name,
            price: item.price,
            image_url: get_image_url(&item.image_name),
            category_id: item.category_id,
            category,
            created_at: item.created_at.timestamp(),
        });
    }

    let mut has_next = false;
    if item_simples.len() > ITEMS_PER_PAGE as usize {
        has_next = true;
        item_simples.truncate(ITEMS_PER_PAGE as usize);
    }

    Ok(web::Json(ResNewItems {
        root_category_id: Some(root_category.id),
        root_category_name: Some(root_category.category_name),
        items: item_simples,
        has_next,
    }))
}

#[get("/users/{user_id}.json")]
async fn get_user_items(
    user_id: web::Path<i64>,
    query: Query,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResUserItems>, Error> {
    let user_id = user_id.into_inner();
    if user_id <= 0 {
        return Err(ErrorBadRequest("incorrect user id").into());
    }

    let mut conn = data.db.get_conn().await?;
    let user_simple = get_user_simple_by_id(&mut conn, user_id)
        .await
        .map_err(|_| ErrorNotFound("user not found"))?;

    assert!(user_simple.id == user_id);

    let fut = if let Query {
        item_id: Some(item_id),
        created_at: Some(created_at),
    } = query
    {
        conn.exec(
 			"SELECT * FROM `items` WHERE `seller_id` = ? AND `status` IN (?,?,?) AND (`created_at` < ?  OR (`created_at` <= ? AND `id` < ?)) ORDER BY `created_at` DESC, `id` DESC LIMIT ?",
            (
                user_simple.id,
                ITEM_STATUS_ON_SALE,
                ITEM_STATUS_TRADING,
                ITEM_STATUS_SOLD_OUT,
                NaiveDateTime::from_timestamp(created_at, 0),
                NaiveDateTime::from_timestamp(created_at, 0),
                item_id,
                ITEMS_PER_PAGE + 1,
            )
        )
    } else {
        conn.exec(
 			"SELECT * FROM `items` WHERE `seller_id` = ? AND `status` IN (?,?,?) ORDER BY `created_at` DESC, `id` DESC LIMIT ?",
            (
                user_simple.id,
                ITEM_STATUS_ON_SALE,
                ITEM_STATUS_TRADING,
                ITEM_STATUS_SOLD_OUT,
                ITEMS_PER_PAGE + 1,
            )
        )
    };

    let items: Vec<Item> = fut.await?;
    let mut item_simples = Vec::<ItemSimple>::with_capacity(items.len());

    for item in items {
        let category = get_category_by_id(&mut conn, item.category_id)
            .await
            .map_err(|_| ErrorNotFound("category not found"))?;

        item_simples.push(ItemSimple {
            id: item.id,
            seller_id: item.seller_id,
            seller: user_simple.clone(),
            status: item.status,
            name: item.name,
            price: item.price,
            image_url: get_image_url(&item.image_name),
            category_id: item.category_id,
            category,
            created_at: item.created_at.timestamp(),
        })
    }

    let mut has_next = false;
    if item_simples.len() > ITEMS_PER_PAGE as usize {
        has_next = true;
        item_simples.truncate(ITEMS_PER_PAGE as usize);
    }

    Ok(web::Json(ResUserItems {
        user: user_simple,
        items: item_simples,
        has_next,
    }))
}

#[get("/users/transactions.json")]
async fn get_transactions(
    session: Session,
    query: Query,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResTransactions>, Error> {
    let mut conn = data.db.get_conn().await?;
    let user = get_user(&mut conn, &session).await?;

    let mut tx = conn.start_transaction(Default::default()).await?;
    let fut = if let Query {
        item_id: Some(item_id),
        created_at: Some(created_at),
    } = query
    {
        // paging
        tx.exec(
 			"SELECT * FROM `items` WHERE (`seller_id` = ? OR `buyer_id` = ?) AND `status` IN (?,?,?,?,?) AND (`created_at` < ?  OR (`created_at` <= ? AND `id` < ?)) ORDER BY `created_at` DESC, `id` DESC LIMIT ?",
            (
                user.id,
                user.id,
                ITEM_STATUS_ON_SALE,
                ITEM_STATUS_TRADING,
                ITEM_STATUS_SOLD_OUT,
                ITEM_STATUS_CANCEL,
                ITEM_STATUS_STOP,
                NaiveDateTime::from_timestamp(created_at, 0),
                NaiveDateTime::from_timestamp(created_at, 0),
                item_id,
                TRANSACTIONS_PER_PAGE + 1,
            )
 		)
    } else {
        tx.exec(
 			"SELECT * FROM `items` WHERE (`seller_id` = ? OR `buyer_id` = ?) AND `status` IN (?,?,?,?,?) ORDER BY `created_at` DESC, `id` DESC LIMIT ?",
            (
                user.id,
                user.id,
                ITEM_STATUS_ON_SALE,
                ITEM_STATUS_TRADING,
                ITEM_STATUS_SOLD_OUT,
                ITEM_STATUS_CANCEL,
                ITEM_STATUS_STOP,
                TRANSACTIONS_PER_PAGE + 1,
            )
 		)
    };

    let items: Vec<Item> = try_or_rollback!(fut.await, tx);
    let mut item_details = Vec::<ItemDetail>::with_capacity(items.len());

    for item in items {
        let seller = try_or_rollback!(
            get_user_simple_by_id(&mut tx, item.seller_id)
                .await
                .map_err(|_| ErrorNotFound("seller not found")),
            tx
        );
        let category = try_or_rollback!(
            get_category_by_id(&mut tx, item.category_id)
                .await
                .map_err(|_| ErrorNotFound("category not found")),
            tx
        );

        let mut item_detail = ItemDetail {
            id: item.id,
            seller_id: item.seller_id,
            seller: seller,
            buyer_id: None,
            buyer: None,
            status: item.status,
            name: item.name,
            price: item.price,
            description: item.description,
            image_url: get_image_url(&item.image_name),
            category_id: item.category_id,
            transaction_evidence_id: None,
            transaction_evidence_status: None,
            shipping_status: None,
            category: category,
            created_at: item.created_at.timestamp(),
        };

        if item.buyer_id != 0 {
            let buyer = try_or_rollback!(
                get_user_simple_by_id(&mut tx, item.buyer_id)
                    .await
                    .map_err(|_| ErrorNotFound("buyer not found")),
                tx
            );
            item_detail.buyer_id = Some(item.buyer_id);
            item_detail.buyer = Some(buyer);
        }

        let transaction_evidence: Option<TransactionEvidence> = try_or_rollback!(
            tx.exec_first(
                "SELECT * FROM `transaction_evidences` WHERE `item_id` = ?",
                (item.id,),
            )
            .await,
            tx
        );

        if let Some(transaction_evidence) = transaction_evidence {
            let shipping: Option<Shipping> = try_or_rollback!(
                tx.exec_first(
                    "SELECT * FROM `shippings` WHERE `transaction_evidence_id` = ?",
                    (transaction_evidence.id,)
                )
                .await,
                tx
            );

            let shipping = try_or_rollback!(
                shipping.ok_or_else(|| ErrorNotFound("shipping not found")),
                tx
            );

            let shipment_service_url = get_shipment_service_url(&mut tx).await;
            let req = api::ShipmentStatusReq {
                reserve_id: shipping.reserve_id,
            };
            let ssr = try_or_rollback!(
                api::shipment_status(&shipment_service_url, &req)
                    .await
                    .map_err(|_| ErrorInternalServerError("failed to request to shipment service")),
                tx
            );

            item_detail.transaction_evidence_id = Some(transaction_evidence.id);
            item_detail.transaction_evidence_status = Some(transaction_evidence.status);
            item_detail.shipping_status = Some(ssr.status);
        }

        item_details.push(item_detail);
    }

    tx.commit().await?;

    let mut has_next = false;
    if item_details.len() > TRANSACTIONS_PER_PAGE as usize {
        has_next = true;
        item_details.truncate(TRANSACTIONS_PER_PAGE as usize);
    }

    Ok(web::Json(ResTransactions {
        items: item_details,
        has_next,
    }))
}

#[get("/items/{item_id}.json")]
async fn get_item(
    item_id: web::Path<i64>,
    session: Session,
    data: web::Data<SharedData>,
) -> Result<web::Json<ItemDetail>, Error> {
    let item_id = item_id.into_inner();
    if item_id <= 0 {
        return Err(ErrorBadRequest("incorrect item id").into());
    }

    let mut conn = data.db.get_conn().await?;
    let user = get_user(&mut conn, &session).await?;

    let res = conn
        .exec_first("SELECT * FROM `items` WHERE `id` = ?", (item_id,))
        .await?;
    let item: Item = match res {
        Some(item) => item,
        None => return Err(ErrorNotFound("item not found").into()),
    };

    let category = get_category_by_id(&mut conn, item.category_id)
        .await
        .map_err(|_| ErrorNotFound("category not found"))?;
    let seller = get_user_simple_by_id(&mut conn, item.seller_id)
        .await
        .map_err(|_| ErrorNotFound("seller not found"))?;

    let mut item_detail = ItemDetail {
        id: item.id,
        seller_id: item.seller_id,
        seller,
        buyer_id: None,
        buyer: None,
        status: item.status,
        name: item.name,
        price: item.price,
        description: item.description,
        image_url: get_image_url(&item.image_name),
        category_id: item.category_id,
        transaction_evidence_id: None,
        transaction_evidence_status: None,
        shipping_status: None,
        category,
        created_at: item.created_at.timestamp(),
    };

    if (user.id == item.seller_id || user.id == item.buyer_id) && item.buyer_id != 0 {
        let buyer = get_user_simple_by_id(&mut conn, item.buyer_id)
            .await
            .map_err(|_| ErrorNotFound("buyer not found"))?;
        item_detail.buyer_id = Some(item.buyer_id);
        item_detail.buyer = Some(buyer);

        let transaction_evidence: Option<TransactionEvidence> = conn
            .exec_first(
                "SELECT * FROM `transaction_evidences` WHERE `item_id` = ?",
                (item.id,),
            )
            .await?;

        if let Some(transaction_evidence) = transaction_evidence {
            let shipping = conn
                .exec_first(
                    "SELECT * FROM `shippings` WHERE `transaction_evidence_id` = ?",
                    (transaction_evidence.id,),
                )
                .await?;
            let shipping: Shipping = shipping.ok_or_else(|| ErrorNotFound("shipping not found"))?;

            item_detail.transaction_evidence_id = Some(transaction_evidence.id);
            item_detail.transaction_evidence_status = Some(transaction_evidence.status);
            item_detail.shipping_status = Some(shipping.status)
        }
    }

    Ok(web::Json(item_detail))
}

#[post("/items/edit")]
async fn post_item_edit(
    rie: web::Json<ReqItemEdit>,
    session: Session,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResItemEdit>, Error> {
    if rie.csrf_token != get_csrf_token(&session)? {
        return Err(ErrorUnprocessableEntity("csrf token error").into());
    }

    if rie.item_price < ITEM_MIN_PRICE || rie.item_price > ITEM_MAX_PRICE {
        return Err(ErrorBadRequest(ITEM_PRICE_ERR_MSG).into());
    }

    let mut conn = data.db.get_conn().await?;
    let seller = get_user(&mut conn, &session).await?;

    let target_item: Item = exec_first_or(
        &mut conn,
        "SELECT * FROM `items` WHERE `id` = ?",
        (rie.item_id,),
        ErrorNotFound("item not found"),
    )
    .await?;

    if target_item.seller_id != seller.id {
        return Err(ErrorForbidden("自分の商品以外は編集できません").into());
    }

    let mut tx = conn.start_transaction(Default::default()).await?;
    let target_item: Item = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `items` WHERE `id` = ? FOR UPDATE",
            (rie.item_id,),
            ErrorNotFound("item not found")
        )
        .await,
        tx
    );

    if target_item.status != ITEM_STATUS_ON_SALE {
        tx.rollback().await?;
        return Err(ErrorForbidden("販売中の商品以外は編集できません").into());
    }

    try_or_rollback!(
        tx.exec_drop(
            "UPDATE `items` SET `price` = ?, `updated_at` = ? WHERE `id` = ?",
            (
                rie.item_price,
                chrono::Local::now().naive_local(),
                rie.item_id,
            )
        )
        .await,
        tx
    );

    let target_item: Item = try_or_rollback!(
        tx.exec_first("SELECT * FROM `items` WHERE `id` = ?", (rie.item_id,))
            .await
            .transpose()
            .unwrap(),
        tx
    );

    tx.commit().await?;

    Ok(web::Json(ResItemEdit {
        item_id: target_item.id,
        item_price: target_item.price,
        item_created_at: target_item.created_at.timestamp(),
        item_updated_at: target_item.updated_at.timestamp(),
    }))
}

#[get("/transactions/{transaction_evidence_id}.png")]
async fn get_qrcode(
    transaction_evidence_id: web::Path<i64>,
    session: Session,
    data: web::Data<SharedData>,
) -> Result<HttpResponse, Error> {
    let transaction_evidence_id = transaction_evidence_id.into_inner();
    if transaction_evidence_id <= 0 {
        return Err(ErrorBadRequest("incorrect transaction_evidence id").into());
    }

    let mut conn = data.db.get_conn().await?;
    let seller = get_user(&mut conn, &session).await?;

    let transaction_evidence: TransactionEvidence = exec_first_or(
        &mut conn,
        "SELECT * FROM `transaction_evidences` WHERE `id` = ?",
        (transaction_evidence_id,),
        ErrorNotFound("transaction_evidences not found"),
    )
    .await?;

    if transaction_evidence.seller_id != seller.id {
        return Err(ErrorForbidden("権限がありません").into());
    }

    let shipping: Shipping = exec_first_or(
        &mut conn,
        "SELECT * FROM `shippings` WHERE `transaction_evidence_id` = ?",
        (transaction_evidence.id,),
        ErrorNotFound("shippings not found"),
    )
    .await?;

    if shipping.status != SHIPPINGS_STATUS_WAIT_PICKUP
        && shipping.status != SHIPPINGS_STATUS_SHIPPING
    {
        return Err(ErrorForbidden("qrcode not available").into());
    }

    if shipping.img_binary.is_empty() {
        return Err(ErrorInternalServerError("empty qrcode image").into());
    }

    Ok(HttpResponse::Ok()
        .content_type("image/png")
        .body(shipping.img_binary))
}

#[post("/buy")]
async fn post_buy(
    rb: web::Json<ReqBuy>,
    session: Session,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResBuy>, Error> {
    let rb = rb.into_inner();
    if rb.csrf_token != get_csrf_token(&session)? {
        return Err(ErrorUnprocessableEntity("csrf token error").into());
    }

    let mut conn = data.db.get_conn().await?;
    let buyer = get_user(&mut conn, &session).await?;

    let mut tx = conn.start_transaction(Default::default()).await?;
    let target_item: Item = exec_first_or(
        &mut tx,
        "SELECT * FROM `items` WHERE `id` = ? FOR UPDATE",
        (rb.item_id,),
        ErrorNotFound("item not found"),
    )
    .await?;
    assert!(target_item.id == rb.item_id);

    if target_item.status != ITEM_STATUS_ON_SALE {
        tx.rollback().await?;
        return Err(ErrorForbidden("item is not for sale").into());
    }

    if target_item.seller_id == buyer.id {
        tx.rollback().await?;
        return Err(ErrorForbidden("自分の商品は買えません").into());
    }

    let seller: User = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `users` WHERE `id` = ? FOR UPDATE",
            (target_item.seller_id,),
            ErrorNotFound("seller not found")
        )
        .await,
        tx
    );

    let category = try_or_rollback!(
        get_category_by_id(&mut tx, target_item.category_id)
            .await
            .map_err(|_| ErrorInternalServerError("category id error")),
        tx
    );

    let result = try_or_rollback!(tx.exec_iter(
        "INSERT INTO `transaction_evidences` (`seller_id`, `buyer_id`, `status`, `item_id`, `item_name`, `item_price`, `item_description`,`item_category_id`,`item_root_category_id`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            target_item.seller_id,
            buyer.id,
            TRANSACTION_EVIDENCE_STATUS_WAIT_SHIPPING,
            target_item.id,
            &target_item.name,
            target_item.price,
            &target_item.description,
            category.id,
            category.parent_id,
        )
 	).await, tx);

    let transaction_evidence_id = try_or_rollback!(
        result
            .last_insert_id()
            .ok_or_else(|| ErrorInternalServerError("db error")),
        tx
    ) as i64;

	try_or_rollback!(
        tx.exec_drop(
            "UPDATE `items` SET `buyer_id` = ?, `status` = ?, `updated_at` = ? WHERE `id` = ?",
            (
                buyer.id,
                ITEM_STATUS_TRADING,
                chrono::Local::now().naive_local(),
                target_item.id,
            )
        ).await,
        tx
    );

    let fut = api::shipment_create(
        &get_shipment_service_url(&mut tx).await,
        &api::ShipmentCreateReq {
            to_address: buyer.address.clone(),
            to_name: buyer.account_name.clone(),
            from_address: seller.address.clone(),
            from_name: seller.account_name.clone(),
        },
    );
    let scr = try_or_rollback!(
        fut.await
            .map_err(|_| ErrorInternalServerError("failed to request to shipment service")),
        tx
    );

    let fut = api::payment_token(
        &get_payment_service_url(&mut tx).await,
        &api::PaymentServiceTokenReq {
            shop_id: PAYMENT_SERVICE_ISUCARI_SHOP_ID.to_string(),
            token: rb.token,
            api_key: PAYMENT_SERVICE_ISUCARI_APIKEY.to_string(),
            price: target_item.price,
        },
    );
    let pstr = try_or_rollback!(
        fut.await
            .map_err(|_| ErrorInternalServerError("payment service is failed")),
        tx
    );

    if pstr.status == "invalid" {
        return Err(ErrorBadRequest("カード情報に誤りがあります").into());
    }

    match &*pstr.status {
        "ok" => {}
        "invalid" => return Err(ErrorBadRequest("カード情報に誤りがあります").into()),
        "fail" => return Err(ErrorBadRequest("カードの残高が足りません").into()),
        _ => return Err(ErrorBadRequest("想定外のエラー").into()),
    };

    try_or_rollback!(tx.exec_drop(
        "INSERT INTO `shippings` (`transaction_evidence_id`, `status`, `item_name`, `item_id`, `reserve_id`, `reserve_time`, `to_address`, `to_name`, `from_address`, `from_name`, `img_binary`) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            transaction_evidence_id,
            SHIPPINGS_STATUS_INITIAL,
            target_item.name,
            target_item.id,
            scr.reserve_id,
            scr.reserve_time,
            buyer.address,
            buyer.account_name,
            seller.address,
            seller.account_name,
            "",
        )
 	).await, tx);

    tx.commit().await?;
    Ok(web::Json(ResBuy {
        transaction_evidence_id,
    }))
}

#[post("/ship")]
async fn post_ship(
    reqps: web::Json<ReqPostShip>,
    session: Session,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResPostShip>, Error> {
    let reqps = reqps.into_inner();
    let csrf_token = reqps.csrf_token;
    let item_id = reqps.item_id;

    if csrf_token != get_csrf_token(&session)? {
        return Err(ErrorUnprocessableEntity("csrf token error").into());
    }

    let mut conn = data.db.get_conn().await?;
    let seller = get_user(&mut conn, &session).await?;

    let transaction_evidence: TransactionEvidence = exec_first_or(
        &mut conn,
        "SELECT * FROM `transaction_evidences` WHERE `item_id` = ?",
        (item_id,),
        ErrorNotFound("transaction_evidences not found"),
    )
    .await?;
    if transaction_evidence.seller_id != seller.id {
        return Err(ErrorForbidden("権限がありません").into());
    }

    let mut tx = conn.start_transaction(Default::default()).await?;

    let item: Item = exec_first_or(
        &mut tx,
        "SELECT * FROM `items` WHERE `id` = ? FOR UPDATE",
        (item_id,),
        ErrorNotFound("item not found"),
    )
    .await?;

    if item.status != ITEM_STATUS_TRADING {
        return Err(ErrorForbidden("商品が取引中ではありません").into());
    }

    let transaction_evidence: TransactionEvidence = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `transaction_evidences` WHERE `id` = ? FOR UPDATE",
            (transaction_evidence.id,),
            ErrorNotFound("transaction_evidences not found")
        )
        .await,
        tx
    );

    if transaction_evidence.status != TRANSACTION_EVIDENCE_STATUS_WAIT_SHIPPING {
        tx.rollback().await?;
        return Err(ErrorForbidden("準備ができていません").into());
    }

    let shipping: Shipping = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `shippings` WHERE `transaction_evidence_id` = ? FOR UPDATE",
            (transaction_evidence.id,),
            ErrorNotFound("shippings not found")
        )
        .await,
        tx
    );

    let fut = api::shipment_request(
        &get_shipment_service_url(&mut tx).await,
        &api::ShipmentRequestReq {
            reserve_id: shipping.reserve_id.clone(),
        },
    );
    let img = try_or_rollback!(
        fut.await
            .map_err(|_| ErrorInternalServerError("failed to request to shipment service")),
        tx
    );

    try_or_rollback!(tx.exec_drop(
        "UPDATE `shippings` SET `status` = ?, `img_binary` = ?, `updated_at` = ? WHERE `transaction_evidence_id` = ?",
        (
            SHIPPINGS_STATUS_WAIT_PICKUP,
            img.as_ref(),
            chrono::Local::now().naive_local(),
            transaction_evidence.id
        )
    ).await, tx);

    tx.commit().await?;

    Ok(web::Json(ResPostShip {
        path: format!("/transactions/{}.png", transaction_evidence.id),
        reserve_id: shipping.reserve_id,
    }))
}

#[post("/ship_done")]
async fn post_ship_done(
    reqpsd: web::Json<ReqPostShipDone>,
    session: Session,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResBuy>, Error> {
    let reqpsd = reqpsd.into_inner();
    let csrf_token = reqpsd.csrf_token;
    let item_id = reqpsd.item_id;

    if csrf_token != get_csrf_token(&session)? {
        return Err(ErrorUnprocessableEntity("csrf token error").into());
    }

    let mut conn = data.db.get_conn().await?;
    let seller = get_user(&mut conn, &session).await?;

    let transaction_evidence: TransactionEvidence = exec_first_or(
        &mut conn,
        "SELECT * FROM `transaction_evidences` WHERE `item_id` = ?",
        (item_id,),
        ErrorNotFound("transaction_evidences not found"),
    )
    .await?;

    if transaction_evidence.seller_id != seller.id {
        return Err(ErrorForbidden("権限がありません").into());
    }

    let mut tx = conn.start_transaction(Default::default()).await?;

    let item: Item = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `items` WHERE `id` = ? FOR UPDATE",
            (item_id,),
            ErrorNotFound("items not found")
        )
        .await,
        tx
    );

    if item.status != ITEM_STATUS_TRADING {
        return Err(ErrorForbidden("商品が取引中ではありません").into());
    }

    let _: Row = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `transaction_evidences` WHERE `id` = ? FOR UPDATE",
            (transaction_evidence.id,),
            ErrorNotFound("transaction_evidences not found")
        )
        .await,
        tx
    );

    if transaction_evidence.status != TRANSACTION_EVIDENCE_STATUS_WAIT_SHIPPING {
        tx.rollback().await?;
        return Err(ErrorForbidden("準備ができていません").into());
    }

    let shipping: Shipping = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `shippings` WHERE `transaction_evidence_id` = ? FOR UPDATE",
            (transaction_evidence.id,),
            ErrorNotFound("shippings not found")
        )
        .await,
        tx
    );

    let fut = api::shipment_status(
        &get_shipment_service_url(&mut tx).await,
        &api::ShipmentStatusReq {
            reserve_id: shipping.reserve_id,
        },
    );
    let ssr = try_or_rollback!(
        fut.await
            .map_err(|_| ErrorInternalServerError("failed to request to shipment service")),
        tx
    );

    if !(ssr.status == SHIPPINGS_STATUS_SHIPPING || ssr.status == SHIPPINGS_STATUS_DONE) {
        tx.rollback().await?;
        return Err(ErrorForbidden("shipment service側で配送中か配送完了になっていません").into());
    }

    try_or_rollback!(
        tx.exec_drop(
            "UPDATE `shippings` SET `status` = ?, `updated_at` = ? WHERE `transaction_evidence_id` = ?",
            (
                ssr.status,
                chrono::Local::now().naive_local(),
                transaction_evidence.id,
            )
        ).await,
        tx
    );

    try_or_rollback!(
        tx.exec_drop(
            "UPDATE `transaction_evidences` SET `status` = ?, `updated_at` = ? WHERE `id` = ?",
            (
                TRANSACTION_EVIDENCE_STATUS_WAIT_DONE,
                chrono::Local::now().naive_local(),
                transaction_evidence.id,
            )
        )
        .await,
        tx
    );

    tx.commit().await?;

    Ok(web::Json(ResBuy {
        transaction_evidence_id: transaction_evidence.id,
    }))
}

#[post("/complete")]
async fn post_complete(
    reqpc: web::Json<ReqPostComplete>,
    session: Session,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResBuy>, Error> {
    let ReqPostComplete {
        csrf_token,
        item_id,
    } = reqpc.into_inner();

    if csrf_token != get_csrf_token(&session)? {
        return Err(ErrorUnprocessableEntity("csrf token error").into());
    }

    let mut conn = data.db.get_conn().await?;
    let buyer = get_user(&mut conn, &session).await?;

    let transaction_evidence_id: TransactionEvidence = exec_first_or(
        &mut conn,
        "select * from `transaction_evidences` where `item_id` = ?",
        (item_id,),
        ErrorNotFound("transaction evidence not found"),
    )
    .await?;

    if transaction_evidence_id.buyer_id != buyer.id {
        return Err(ErrorForbidden("権限がありません").into());
    }

    let mut tx = conn.start_transaction(Default::default()).await?;
    let item: Item = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `items` WHERE `id` = ? FOR UPDATE",
            (item_id,),
            ErrorNotFound("items not found")
        )
        .await,
        tx
    );

    if item.status != ITEM_STATUS_TRADING {
        tx.rollback().await?;
        return Err(ErrorForbidden("商品が取引中ではありません").into());
    }

    let transaction_evidence: TransactionEvidence = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `transaction_evidences` WHERE `item_id` = ? FOR UPDATE",
            (item_id,),
            ErrorNotFound("transaction_evidences not found")
        )
        .await,
        tx
    );

    if transaction_evidence.status != TRANSACTION_EVIDENCE_STATUS_WAIT_DONE {
        tx.rollback().await?;
        return Err(ErrorForbidden("準備ができていません").into());
    }

    let shipping: Shipping = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `shippings` WHERE `transaction_evidence_id` = ? FOR UPDATE",
            (transaction_evidence.id,),
            ErrorNotFound("shippings not found")
        )
        .await,
        tx
    );

    let fut = api::shipment_status(
        &get_shipment_service_url(&mut tx).await,
        &api::ShipmentStatusReq {
            reserve_id: shipping.reserve_id,
        },
    );
    let ssr = try_or_rollback!(
        fut.await
            .map_err(|_| ErrorInternalServerError("failed to request to shipment service")),
        tx
    );

    if ssr.status != SHIPPINGS_STATUS_DONE {
        tx.rollback().await?;
        return Err(ErrorBadRequest("shipment service側で配送完了になっていません").into());
    }

    try_or_rollback!(
        tx.exec_drop(
            "UPDATE `shippings` SET `status` = ?, `updated_at` = ? WHERE `transaction_evidence_id` = ?",
            (
                SHIPPINGS_STATUS_DONE,
                chrono::Local::now().naive_local(),
                transaction_evidence.id,
            )
        ).await,
        tx
    );

    try_or_rollback!(
        tx.exec_drop(
            "UPDATE `transaction_evidences` SET `status` = ?, `updated_at` = ? WHERE `id` = ?",
            (
                TRANSACTION_EVIDENCE_STATUS_DONE,
                chrono::Local::now().naive_local(),
                transaction_evidence.id,
            )
        )
        .await,
        tx
    );

    try_or_rollback!(
        tx.exec_drop(
            "UPDATE `items` SET `status` = ?, `updated_at` = ? WHERE `id` = ?",
            (
                ITEM_STATUS_SOLD_OUT,
                chrono::Local::now().naive_local(),
                item_id,
            )
        )
        .await,
        tx
    );

    tx.commit().await?;
    Ok(web::Json(ResBuy {
        transaction_evidence_id: transaction_evidence.id,
    }))
}

#[post("/sell")]
async fn post_sell(
    mut form: awmp::Parts,
    session: Session,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResSell>, Error> {
    let text_parts = form.texts.as_hash_map();
    let get = |key: &str| -> _ {
        text_parts
            .get(key)
            .ok_or(ErrorBadRequest("all parameters are required"))
    };
    let csrf_token = get("csrf_token")?;
    let name = get("name")?;
    let description = get("description")?;
    let price_str = get("price")?;
    let category_id_str = get("category_id")?;

    let mut f = match form.files.take("image").pop() {
        Some(f) => f,
        None => return Err(ErrorBadRequest("image error").into()),
    };

    if *csrf_token != get_csrf_token(&session)? {
        return Err(ErrorUnprocessableEntity("csrf token error").into());
    }

    let category_id = match category_id_str.parse() {
        Ok(val @ 1..=i64::MAX) => val,
        _ => return Err(ErrorBadRequest("category id error").into()),
    };

    let price: i64 = price_str
        .parse()
        .map_err(|_| ErrorBadRequest("price errror"))?;

    if price < ITEM_MIN_PRICE || price > ITEM_MAX_PRICE {
        return Err(ErrorBadRequest(ITEM_PRICE_ERR_MSG).into());
    }

    let mut conn = data.db.get_conn().await?;
    let category = match get_category_by_id(&mut conn, category_id).await {
        Ok(category) if category.parent_id != 0 => category,
        _ => return Err(ErrorBadRequest("Incorrect category ID").into()),
    };

    let user = get_user(&mut conn, &session).await?;
    let mut img = Vec::new();
    let _ = f.as_mut().seek(SeekFrom::Start(0));
    f.as_mut()
        .read_to_end(&mut img)
        .map_err(|_| ErrorInternalServerError("image error"))?;

    let mut ext = match f.original_file_name() {
        Some(s) => {
            let idx = s.find('.').unwrap_or(s.len());
            &s[idx..]
        }
        None => return Err(ErrorBadRequest("cannot detect image type").into()),
    };

    if !(ext == ".jpg" || ext == ".jpeg" || ext == ".png" || ext == ".gif") {
        return Err(ErrorBadRequest("unsupported image format error").into());
    }

    if ext == ".jpeg" {
        ext = ".jpg"
    }

    let img_name = format!("{}{}", secure_random_str(16), ext);
    let out_file = format!("../public/upload/{}", img_name);
    tokio::fs::write(&out_file, img)
        .await
        .map_err(|_| ErrorInternalServerError("Saving image failed"))?;
    #[cfg(unix)]
    let _ = fs::set_permissions(&out_file, fs::Permissions::from_mode(0o644));

    let mut tx = conn.start_transaction(Default::default()).await?;

    let seller: User = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `users` WHERE `id` = ? FOR UPDATE",
            (user.id,),
            ErrorNotFound("user not found")
        )
        .await,
        tx
    );

    let result = tx.exec_iter("INSERT INTO `items` (`seller_id`, `status`, `name`, `price`, `description`,`image_name`,`category_id`) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            seller.id,
            ITEM_STATUS_ON_SALE,
            name,
            price,
            description,
            img_name,
            category.id,
        )
 	).await?;

    let item_id = result
        .last_insert_id()
        .ok_or_else(|| ErrorInternalServerError("db error"))? as i64;
    let now = chrono::Local::now().naive_local();

    tx.exec_drop(
        "UPDATE `users` SET `num_sell_items`=?, `last_bump`=? WHERE `id`=?",
        (seller.num_sell_items + 1, now, seller.id),
    )
    .await?;

    tx.commit().await?;
    Ok(web::Json(ResSell { id: item_id }))
}

fn secure_random_str(len: usize) -> String {
    const LETTERS: &[u8; 36] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = SmallRng::from_entropy();

    unsafe {
        let collects: Vec<u8> = (0..len)
            .map(|_| *LETTERS.get_unchecked((((rng.next_u32() as u64) * 36) >> 32) as usize))
            .collect();
        String::from_utf8_unchecked(collects)
    }
}

#[post("/bump")]
async fn post_bump(
    rb: web::Json<ReqBump>,
    session: Session,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResItemEdit>, Error> {
    let ReqBump {
        csrf_token,
        item_id,
    } = rb.into_inner();
    if csrf_token != get_csrf_token(&session)? {
        return Err(ErrorBadRequest("json decode error").into());
    }

    let mut conn = data.db.get_conn().await?;
    let user = get_user(&mut conn, &session).await?;

    let mut tx = conn.start_transaction(Default::default()).await?;

    let target_item: Item = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `items` WHERE `id` = ? FOR UPDATE",
            (item_id,),
            ErrorNotFound("item not found")
        )
        .await,
        tx
    );

    if target_item.seller_id != user.id {
        tx.rollback().await?;
        return Err(ErrorForbidden("自分の商品以外は編集できません").into());
    }

    let seller: User = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `users` WHERE `id` = ? FOR UPDATE",
            (user.id,),
            ErrorNotFound("user not found")
        )
        .await,
        tx
    );

    let now = chrono::Local::now().naive_local();
    if seller.last_bump + Duration::seconds(BUMP_CHARGE_SECONDS) > now {
        tx.rollback().await?;
        return Err(ErrorForbidden("Bump not allowed").into());
    }

    try_or_rollback!(
        tx.exec_drop(
            "UPDATE `items` SET `created_at`=?, `updated_at`=? WHERE id=?",
            (now, now, target_item.id,)
        )
        .await,
        tx
    );

    try_or_rollback!(
        tx.exec_drop(
            "UPDATE `users` SET `last_bump`=? WHERE id=?",
            (now, seller.id),
        )
        .await,
        tx
    );

    let target_item: Item = try_or_rollback!(
        exec_first_or(
            &mut tx,
            "SELECT * FROM `items` WHERE `id` = ?",
            (item_id,),
            ErrorNotFound("item not found")
        )
        .await,
        tx
    );

    tx.commit().await?;
    Ok(web::Json(ResItemEdit {
        item_id: target_item.id,
        item_price: target_item.price,
        item_created_at: target_item.created_at.timestamp(),
        item_updated_at: target_item.updated_at.timestamp(),
    }))
}

#[get("/settings")]
async fn get_settings(
    session: Session,
    data: web::Data<SharedData>,
) -> Result<web::Json<ResSetting>, Error> {
    let csrf_token = get_csrf_token(&session)?;
    let mut conn = data.db.get_conn().await?;
    let user = get_user(&mut conn, &session).await.ok();

    let payment_service_url = get_payment_service_url(&mut conn).await;
    let categories: Vec<Category> = conn.query("SELECT * FROM `categories`").await?;

    Ok(web::Json(ResSetting {
        csrf_token,
        user,
        payment_service_url,
        categories,
    }))
}

#[post("/login")]
async fn post_login(
    rl: web::Json<ReqLogin>,
    session: Session,
    data: web::Data<SharedData>,
) -> Result<web::Json<User>, Error> {
    let ReqLogin {
        account_name,
        password,
    } = rl.into_inner();

    let mut conn = data.db.get_conn().await?;
    let u: User = exec_first_or(
        &mut conn,
        "SELECT * FROM `users` WHERE `account_name` = ?",
        (account_name,),
        ErrorUnauthorized("アカウント名かパスワードが間違えています"),
    )
    .await?;

    if !bcrypt::verify(&password, &u.hashed_password).map_err(|_| ErrorInternalServerError("crypt error"))? {
        return Err(ErrorUnauthorized("アカウント名かパスワードが間違えています").into());
    }

    session
        .set(
            SESSION_NAME,
            UserSession {
                user_id: u.id,
                csrf_token: secure_random_str(20),
            },
        )
        .map_err(|_| ErrorInternalServerError("session error"))?;

    Ok(web::Json(u))
}

#[post("/register")]
async fn post_register(
    rr: web::Json<ReqRegister>,
    session: Session,
    data: web::Data<SharedData>,
) -> Result<web::Json<User>, Error> {
    let ReqRegister {
        account_name,
        address,
        password,
    } = rr.into_inner();

    let hashed_password = bcrypt::hash(&password, 10).map_err(|_| ErrorInternalServerError("error"))?;

    let mut conn = data.db.get_conn().await?;

    let result = conn
        .exec_iter(
            "INSERT INTO `users` (`account_name`, `hashed_password`, `address`) VALUES (?, ?, ?)",
            (&*account_name, &*hashed_password, &*address),
        )
        .await?;

    let user_id = result
        .last_insert_id()
        .ok_or_else(|| ErrorInternalServerError("db error"))? as i64;

    let u = User {
        id: user_id,
        hashed_password,
        account_name,
        address,
        num_sell_items: 0,
        last_bump: NaiveDateTime::from_timestamp(0, 0), // dummy value
        created_at: NaiveDateTime::from_timestamp(0, 0), // dummy value
    };

    session
        .set(
            SESSION_NAME,
            UserSession {
                user_id: u.id,
                csrf_token: secure_random_str(20),
            },
        )
        .map_err(|_| ErrorInternalServerError("session error"))?;

    Ok(web::Json(u))
}

#[get("/reports.json")]
async fn get_reports(
    data: web::Data<SharedData>,
) -> Result<web::Json<Vec<TransactionEvidence>>, Error> {
    let mut conn = data.db.get_conn().await?;
    let transaction_evidences: Vec<TransactionEvidence> = conn
        .query("SELECT * FROM `transaction_evidences` WHERE `id` > 15007")
        .await?;
    Ok(web::Json(transaction_evidences))
}

#[actix_rt::main]
async fn main() -> Result<(), std::io::Error> {
    let host = env::var("MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = match env::var("MYSQL_PORT") {
        Ok(port) => port
            .parse()
            .unwrap_or_else(|_| panic!("Invalid port number: {}", port)),
        Err(_) => 3306_u32,
    };
    let user = env::var("MYSQL_USER").unwrap_or_else(|_| "isucari".to_string());
    let dbname = env::var("MYSQL_DBNAME").unwrap_or_else(|_| "isucari".to_string());
    let password = env::var("MYSQL_PASS").unwrap_or_else(|_| "isucari".to_string());

    let mysql_url = format!("mysql://{}:{}@{}:{}/{}", user, password, host, port, dbname);
    let pool = mysql_async::Pool::new(mysql_url);

    let shared_data = web::Data::new(SharedData { db: pool });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(CookieSession::signed(&[0; 32]).secure(false))
            .app_data(shared_data.clone())
            // API
            .service(post_initialize)
            .service(get_new_items)
            .service(get_new_category_items)
            .service(get_transactions)
            .service(get_user_items)
            .service(get_item)
            .service(post_item_edit)
            .service(post_buy)
            .service(post_sell)
            .service(post_ship)
            .service(post_ship_done)
            .service(post_complete)
            .service(get_qrcode)
            .service(post_bump)
            .service(get_settings)
            .service(post_login)
            .service(post_register)
            .service(get_reports)
            // frontend
            .route("/", web::get().to(get_index))
            .route("/login", web::get().to(get_index))
            .route("/register", web::get().to(get_index))
            .route("/timeline", web::get().to(get_index))
            .route("/categories/{category_id}/items", web::get().to(get_index))
            .route("/sell", web::get().to(get_index))
            .route("/items/{item_id}", web::get().to(get_index))
            .route("/items/{item_id}/edit", web::get().to(get_index))
            .route("/items/{item_id}/buy", web::get().to(get_index))
            .route("/buy/complete", web::get().to(get_index))
            .route("/transactions/{transaction_id}", web::get().to(get_index))
            .route("/users/{user_id}", web::get().to(get_index))
            .route("/users/setting", web::get().to(get_index))
            // assets
            .service(Files::new("/", "../public/").index_file("index.html"))
    })
    .workers(16)
    .bind("127.0.0.1:8000")?
    .run()
    .await
}
