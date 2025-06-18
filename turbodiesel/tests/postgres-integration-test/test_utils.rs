use std::sync::Once;

use dotenvy::dotenv;

static INIT: Once = Once::new();

pub fn init_logging_for_tests() {
    INIT.call_once(|| {
        dotenv().ok();
        let _ = env_logger::builder()
            .target(env_logger::Target::Stdout)
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();
    });
}
