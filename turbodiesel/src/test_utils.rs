use std::sync::Once;

static INIT: Once = Once::new();

pub fn init_logging_for_tests() {
    INIT.call_once(|| {
        let _ = env_logger::builder()
            .target(env_logger::Target::Stdout)
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
    });
}
