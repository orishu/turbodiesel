use std::sync::Once;

use dockertest::DockerOperations;
use dockertest::{DockerTest, TestBodySpecification};
use dotenvy::dotenv;
use log::info;
use port_check::free_local_ipv4_port;

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
        f(redis_url, ops).await;
    });
    info!("Finished running inside Redis.");
}
