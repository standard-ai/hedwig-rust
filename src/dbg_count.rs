use std::collections::BTreeMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use std::time::Instant;

struct CallCounter {
    count: u32,
    last_reset: Instant,
}

impl CallCounter {
    fn new() -> Self {
        CallCounter {
            count: 0,
            last_reset: Instant::now(),
        }
    }

    fn update_and_print_rate(&mut self, key: &'static str) {
        self.count += 1;

        if self.last_reset.elapsed() >= Duration::from_secs(1) {
            println!("{} called {} times in the last second", key, self.count);
            self.count = 0;
            self.last_reset = Instant::now();
        }
    }
}

static COUNTER: LazyLock<Mutex<BTreeMap<&'static str, CallCounter>>> =
    LazyLock::new(|| Mutex::new(Default::default()));

pub(crate) fn update_and_print_rate(key: &'static str) {
    COUNTER
        .lock()
        .unwrap()
        .entry(key)
        .or_insert_with(CallCounter::new)
        .update_and_print_rate(key);
}
