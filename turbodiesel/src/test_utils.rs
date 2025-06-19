use std::sync::Once;

use async_std::task;
use dockertest::DockerOperations;
use dockertest::{DockerTest, TestBodySpecification};
use dotenvy::dotenv;
use log::info;
use port_check::free_local_ipv4_port;
use redis::Client;
use redis::Commands;
use redis::RedisError;
use std::time::Duration;

static INIT: Once = Once::new();

pub fn init_logging_for_tests() {
    INIT.call_once(|| {
        dotenv().ok();
        let _ = env_logger::builder()
            .target(env_logger::Target::Stdout)
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
    });
}

pub fn run_with_redis<Fun, Fut>(f: Fun)
where
    Fut: Future<Output = ()> + Send + 'static,
    Fun: FnOnce(String, DockerOperations) -> Fut + Send + 'static,
{
    let image = dockertest::Image::with_repository("redis").source(dockertest::Source::DockerHub);
    let mut redis_container = TestBodySpecification::with_image(image);
    let port = free_local_ipv4_port().unwrap();
    let redis_url = format!("redis://localhost:{}", port);
    redis_container.modify_port_map(6379, port.into());
    let mut test = DockerTest::new();
    test.provide_container(redis_container);
    info!("Running inside Redis: {}", redis_url);
    test.run(|ops| async move {
        let redis_client =
            redis::Client::open(redis_url.clone()).expect("cannot open connection to redis");
        wait_until_redis_online(&redis_client, 6)
            .await
            .expect("redis is not online");
        load_redis_functions(&redis_client).expect("failed loading redis functions");
        f(redis_url, ops).await;
    });
    info!("Finished running inside Redis.");
}

fn check_redis_online(client: &Client) -> bool {
    match client.get_connection() {
        Ok(mut con) => con.ping::<String>().is_ok(),
        Err(_) => false,
    }
}

async fn wait_until_redis_online(client: &Client, retries: usize) -> Result<(), RedisError> {
    for _ in 0..retries {
        if check_redis_online(client) {
            return Ok(());
        }
        task::sleep(Duration::from_secs(1)).await;
    }
    Err(RedisError::from((
        redis::ErrorKind::IoError,
        "Redis is not online",
    )))
}

fn load_redis_functions(client: &Client) -> Result<(), RedisError> {
    let script = include_str!("../lua/functions.lua");
    let mut con = client.get_connection()?;
    con.send_packed_command(
        redis::cmd("FUNCTION")
            .arg("LOAD")
            .arg("REPLACE")
            .arg(script)
            .get_packed_command()
            .as_slice(),
    )?;
    let response = con.recv_response()?;
    info!("Loaded Redis functions for module: {:?}", response);
    Ok(())
}
