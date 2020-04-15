use chrono::{DateTime, Duration /*TimeZone*/, Local};
use env_logger;
use env_logger::Env;
use main_error::MainError;
use std::env;
use structopt::StructOpt;
use tokio;
use tokio::process::Command;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;
use tokio_postgres::NoTls;

#[derive(Debug, StructOpt)]
#[structopt(name = "cleaniit", about = "clean up idle in transaction processes")]
struct Opt {
    /// Set the log level. This may target one or more
    /// specific modules or be general.
    /// (levels: trace, debug, info, warn, error)
    #[structopt(long)]
    pub loglevel: Option<String>,
    /// Activate debug mode
    // short and long flags (-d, --debug) will be deduced from the field's name
    #[structopt(short, long)]
    debug: bool,
    /// Kill idle in transaction process
    #[structopt(short = "k", long = "kill")]
    kill: bool,
    /// Minimum age in minutes of process since last change
    #[structopt(short = "a", long = "min-age")]
    min_age: Option<i64>,
    /// max number of processes killed
    #[structopt(short = "m", long = "max")]
    max_killed: Option<i64>,
    ///Dry run
    #[structopt(short = "n", long = "dry-run")]
    dry_run: bool,
}

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
    let opt = Opt::from_args();
    let Opt {
        loglevel: maybe_level,
        kill,
        min_age,
        max_killed,
        dry_run,
        ..
    } = opt;
    if let Some(ref level) = maybe_level {
        env::set_var("RUST_LOG", level);
    }

    // set default min age of two hours
    let min_age = min_age.unwrap_or(120);

    env_logger::from_env(Env::default().default_filter_or("info")).init();

    let (client, connection) = tokio_postgres::connect(
        "host=pd-organic-db-01.d2.com user=postgres password=CZf942Z64XGsGRngJk port=5432",
        NoTls,
    )
    .await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log::error!("connection error: {}", e);
        }
    });
    // make the call
    let rows = find_iit(&client).await?;
    let max_k = max_killed.unwrap_or(rows.len() as i64);
    let mut cnt = 0;
    for row in rows {
        log::info!("{:#?}", row);
        let age = age(row.state_change);
        log::info!("Age {}", age);
        if kill && age.num_minutes() > min_age {
            if cnt < max_k {
                log::info!("killing {}", row.pid);
                if !dry_run {
                    Command::new("sudo")
                        .arg("/usr/bin/pkill")
                        .arg("-f")
                        .arg("'idle in transaction'")
                        .arg("-o")
                        .arg("-f")
                        .arg("-e")
                        .spawn()
                        .expect("unable to pkill")
                        .await?;
                }
                cnt += 1;
            }
        }
    }

    Ok(())
}
