use async_std::task;
use diesel::dsl;
use diesel::prelude::PgConnection;
use diesel::sql_types::Integer;
use diesel::{Connection, RunQueryDsl};
use dockertest::DockerOperations;
use dockertest::{DockerTest, TestBodySpecification};
use log::info;
use port_check::free_local_ipv4_port;
use std::error::Error;
use std::time::Duration;

pub struct PostgresTestUtil {}

impl PostgresTestUtil {
    pub fn new() -> Self {
        PostgresTestUtil {}
    }

    pub fn run_test_with_postgres<Fun, Fut>(&self, f: Fun)
    where
        Fut: Future<Output = ()> + Send + 'static,
        Fun: FnOnce(String, DockerOperations) -> Fut + Send + 'static,
    {
        let mut test = DockerTest::new();
        let image =
            dockertest::Image::with_repository("postgres").source(dockertest::Source::DockerHub);
        let mut container = TestBodySpecification::with_image(image);
        let port = free_local_ipv4_port().unwrap();
        let url = format!("postgres://postgres@localhost:{}/postgres", port);
        container.modify_port_map(5432, port.into());
        container.modify_env("POSTGRES_HOST_AUTH_METHOD", "trust");
        test.provide_container(container);
        info!("Running inside Postgres: {}", url);
        test.run(async move |ops| {
            Self::wait_until_postgres_online(&url, 6)
                .await
                .expect("postgres is not online");
            f(url, ops).await;
        });
        info!("Finished running inside Redis.");
    }

    async fn wait_until_postgres_online(
        url: &String,
        retries: usize,
    ) -> Result<(), Box<dyn Error>> {
        for i in 0..retries {
            let con_res = PgConnection::establish(&url).map_err(|e| Box::new(e));
            if let Ok(mut con) = con_res {
            let res = diesel::select(dsl::sql::<Integer>("1")).execute(&mut con);
            match res {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if i == retries - 1 {
                        return Err(Box::new(e));
                    }
                }
            }
        }
            task::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_connect() {
        let util = PostgresTestUtil::new();

        util.run_test_with_postgres(async move |url, _| {
            let mut con = PgConnection::establish(&url).expect("Error connecting to postgres");
            let result = diesel::select(dsl::sql::<Integer>("1"))
                .get_result::<i32>(&mut con)
                .unwrap();
            assert_eq!(result, 1);
        });
    }
}
