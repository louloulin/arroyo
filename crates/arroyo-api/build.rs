use cornucopia::{CodegenSettings, Error};
use postgres::{Client, NoTls};

fn main() -> Result<(), Error> {
    let queries_path = "queries";
    let destination = format!("{}/api-sql.rs", std::env::var("OUT_DIR").unwrap());
    let settings = CodegenSettings {
        gen_async: true,
        derive_ser: true,
        gen_sync: false,
        gen_sqlite: true,
    };

    println!("cargo:rerun-if-changed={queries_path}");
    println!("cargo:rerun-if-changed=migrations");
    println!("cargo:rerun-if-changed=sqlite_migrations");

    // 从环境变量获取PostgreSQL端口，默认为5433
    let pg_port = std::env::var("ARROYO_POSTGRES_PORT")
        .unwrap_or_else(|_| "5433".to_string())
        .parse::<u16>()
        .unwrap_or(5433);

    println!("cargo:warning=Using PostgreSQL port: {}", pg_port);

    // 尝试连接到PostgreSQL
    let client_result = Client::configure()
        .dbname("arroyo")
        .host("localhost")
        .port(pg_port)
        .user("arroyo")
        .password("arroyo")
        .connect(NoTls);

    // 如果无法连接到PostgreSQL，则使用SQLite
    let mut client = match client_result {
        Ok(client) => {
            println!("cargo:warning=Successfully connected to PostgreSQL");
            client
        },
        Err(e) => {
            println!("cargo:warning=Failed to connect to PostgreSQL: {}. Using SQLite instead.", e);
            // 创建一个空的Client，后面会使用SQLite
            panic!("Could not connect to postgres, please use SQLite instead")
        }
    };

    let mut sqlite =
        rusqlite::Connection::open_in_memory().expect("Couldn't open sqlite memory connection");
    let migrations = refinery::load_sql_migrations("sqlite_migrations").unwrap();
    refinery::Runner::new(&migrations)
        .run(&mut sqlite)
        .expect("Failed to run migrations");

    cornucopia::generate_live_with_sqlite(
        &mut client,
        queries_path,
        Some(&destination),
        &sqlite,
        settings,
    )?;

    Ok(())
}
