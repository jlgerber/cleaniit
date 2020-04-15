use chrono::{DateTime, Duration /*TimeZone*/, Local};
use env_logger;
use env_logger::Env;
use main_error::MainError;
use std::env;
use tokio;
use tokio::process::Command;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;
use tokio_postgres::NoTls;

#[derive(Debug, PartialEq, Eq)]
pub struct FindIitRow {
    pub datid: i64,
    pub pid: i64,
    pub query: String,
    pub backend_start: DateTime<Local>,
    pub xact_start: DateTime<Local>,
    pub query_start: DateTime<Local>,
    pub state_change: DateTime<Local>,
}

fn age(start: DateTime<Local>) -> Duration {
    Local::now() - start
}

async fn find_iit(client: &Client) -> Result<Vec<FindIitRow>, Box<dyn std::error::Error>> {
    let mut results = Vec::new();
    let query_str =
        "SELECT datid,pid,query,backend_start,xact_start,query_start,state_change from pg_stat_activity where state = 'idle in transaction' order by state_change";
    for row in client.query(query_str, &[]).await? {
        let datid: i64 = row.get(0);
        let pid: i64 = row.get(1);
        let query: &str = row.get(2);
        let backend_start: DateTime<Local> = row.get(3);
        let xact_start: DateTime<Local> = row.get(4);
        let query_start: DateTime<Local> = row.get(5);
        let state_change: DateTime<Local> = row.get(6);
        results.push(FindIitRow {
            datid,
            pid,
            query: query.to_string(),
            backend_start,
            xact_start,
            query_start,
            state_change,
        });
    }

    Ok(results)
}
#[tokio::main]
async fn main() -> Result<(), MainError> {
    //let level = "debug";
    //env::set_var("RUST_LOG", level);

    env_logger::from_env(Env::default().default_filter_or("warn")).init();

    let (client, connection) = tokio_postgres::connect(
        "host=pd-organic-db-01.d2.com user=postgres password=CZf942Z64XGsGRngJk port=5432",
        NoTls,
    )
    .await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    // make the call
    let rows = find_iit(&client).await?;
    for row in rows {
        println!("{:#?}", row);
        println!("Age {}", age(row.state_change));
    }

    Ok(())
}
