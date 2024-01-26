use actix_web::{web, App, HttpResponse, HttpServer, Responder, post, get};
use sqlx::{SqlitePool, FromRow, migrate::MigrateDatabase, Sqlite, Row};
use serde::{Serialize, Deserialize};
use std::net::TcpListener;

const DB_URL: &str = "sqlite://sqlite.db";

#[derive(Clone, FromRow, Serialize, Deserialize, Debug)]
struct Argument {
    id: String,
    name: String,
    _type: String,
}

#[derive(Clone, FromRow, Serialize, Deserialize, Debug)]
struct Service {
    id: String,
    name: String,
    arguments: Vec<Argument>,
}

#[derive(Clone, FromRow, Serialize, Deserialize, Debug)]
struct Place {
    id: String,
    name: String,
    services: Vec<Service>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotBody {
    places: Vec<Place>,
}

#[post("/snapshot")]
async fn update_snapshot(pool: web::Data<SqlitePool>, snapshot: web::Json<SnapshotBody>) -> impl Responder {
    // Erase previous data
    sqlx::query("DELETE FROM arguments", ).execute(pool.get_ref()).await.unwrap();
    sqlx::query("DELETE FROM services", ).execute(pool.get_ref()).await.unwrap();
    sqlx::query("DELETE FROM places", ).execute(pool.get_ref()).await.unwrap();

    // Store new data
    for place in &snapshot.places {
        sqlx::query("INSERT INTO places (id, name) VALUES ($1, $2)").bind(&place.id).bind(&place.name).execute(pool.get_ref()).await.unwrap();
        for service in &place.services {
            sqlx::query("INSERT INTO services (id, name, place_id) VALUES ($1, $2, $3)").bind(&service.id).bind(&service.name).bind(&place.id).execute(pool.get_ref()).await.unwrap();
            for argument in &service.arguments {
                sqlx::query("INSERT INTO arguments (id, name, type, service_id) VALUES ($1, $2, $3, $4)").bind(&argument.id).bind(&argument.name).bind(&argument._type).bind(&service.id).execute(pool.get_ref()).await.unwrap();
            }
        }
    }
    HttpResponse::Ok().finish()
}

#[get("/snapshot")]
async fn retrieve_snapshot(pool: web::Data<SqlitePool>) -> impl Responder {
    let mut snapshot_body: SnapshotBody = SnapshotBody { places: vec![] };

    let place_records = sqlx::query("SELECT id, name FROM places")
        .fetch_all(pool.get_ref())
        .await
        .unwrap();

    for place in place_records {

        let place_id: Result<String, _> = place.try_get("id");

        let service_records = sqlx::query("SELECT id, name FROM services WHERE place_id = ?")
            .bind(place_id.unwrap())
            .fetch_all(pool.get_ref())
            .await
            .unwrap();

        let mut service_structs: Vec<Service> = vec![];

        for service in service_records {

            let service_id: Result<String, _> = service.try_get("id");

            let argument_records = sqlx::query("SELECT id, name, type as _type FROM arguments WHERE service_id = ?")
                .bind(service_id.unwrap())
                .fetch_all(pool.get_ref())
                .await
                .unwrap();

            let argument_structs: Vec<Argument> = argument_records.into_iter()
                .map(|arg| Argument { id: arg.try_get("id").unwrap(), name: arg.try_get("name").unwrap(), _type: arg.try_get("_type").unwrap() }) // Please replace _type and arg_type with the actual name you used.
                .collect();

            let service_id: Result<String, _> = service.try_get("id");
            let service_name: Result<String, _> = service.try_get("name");
            service_structs.push(Service { id: service_id.unwrap(), name: service_name.unwrap(), arguments: argument_structs });
        }

        snapshot_body.places.push(Place { id: place.try_get("id").unwrap(), name: place.try_get("name").unwrap(), services: service_structs });
    }

    HttpResponse::Ok().json(snapshot_body)
}

async fn health_check() -> HttpResponse {
    HttpResponse::Ok().finish()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();

    if !Sqlite::database_exists(DB_URL).await.unwrap_or(false) {
        println!("Creating database {}", DB_URL);
        match Sqlite::create_database(DB_URL).await {
            Ok(_) => println!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    } else {
        println!("Database already exists");
    }

    let pool = SqlitePool::connect(DB_URL).await.unwrap();

    sqlx::query(r"
        CREATE TABLE IF NOT EXISTS places (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS services (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            place_id UUID REFERENCES places(id)
        );
        CREATE TABLE IF NOT EXISTS arguments (
            id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            service_id UUID REFERENCES services(id)
        );
    ").execute(&pool).await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:8889")?;

    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .service(update_snapshot)
            .service(retrieve_snapshot)
            .service(
                web::scope("/health_check")
                    .route("", web::get().to(health_check))
            )
    }).listen(listener)?.run().await?;

    Ok(())
}