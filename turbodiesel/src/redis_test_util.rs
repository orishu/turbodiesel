use async_std::task;
use dockertest::DockerOperations;
use dockertest::{DockerTest, TestBodySpecification};
use log::info;
use port_check::free_local_ipv4_port;
use redis::Client;
use redis::Commands;
use redis::RedisError;
use std::time::Duration;

pub struct RedisTestUtil {
    client: redis::Client,
    url: String,
    port: u16,
}

impl RedisTestUtil {
    pub fn new() -> Self {
        let port = free_local_ipv4_port().unwrap();
        let url = format!("redis://localhost:{}", port);
        let client = redis::Client::open(url.clone()).expect("cannot create redis client");
        RedisTestUtil { client, url, port }
    }

    pub fn run_test_with_redis<Fun, Fut>(&self, f: Fun)
    where
        Fut: Future<Output = ()> + Send + 'static,
        Fun: FnOnce(String, DockerOperations) -> Fut + Send + 'static,
    {
        let mut test = DockerTest::new();
        let image =
            dockertest::Image::with_repository("redis").source(dockertest::Source::DockerHub);
        let mut container = TestBodySpecification::with_image(image);
        container.modify_port_map(6379, self.port.into());
        test.provide_container(container);
        info!("Running inside Redis: {}", self.url);
        let client = self.client.clone();
        let url = self.url.clone();
        test.run(|ops| async move {
            Self::wait_until_redis_online(&client, 6)
                .await
                .expect("redis is not online");
            Self::load_redis_functions(&client).expect("failed loading redis functions");
            f(url, ops).await;
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
            if Self::check_redis_online(client) {
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
}
