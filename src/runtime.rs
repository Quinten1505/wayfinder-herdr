use crate::{
    host,
    store::{self, Lock, State},
};
use anyhow::Result;
use std::{
    path::Path,
    thread,
    time::{Duration, Instant},
};

/// systemd supervises this process; the kernel releases its lifetime lock on death.
pub fn serve(root: &Path, key: &str, once: bool) -> Result<()> {
    let dir = store::map_dir(root, key)?;
    let _runtime_lock = Lock::acquire(&dir.join("runtime.lock"))?;
    let mut next_check = Instant::now();
    let mut failures = 0u32;
    loop {
        // Commands never write state while the runtime owns this transaction lock.
        {
            let _state_lock = Lock::acquire(&dir.join("state.lock"))?;
            let mut state = store::read_state(&dir)?;
            let before = state.history.len();
            store::process_requests(&dir, &mut state)?;
            if before != state.history.len() || Instant::now() >= next_check {
                let result = host::check(&state.binding).and_then(|()| {
                    let map = crate::tracker::MapRef::parse(&state.map)?;
                    let frontier = crate::tracker::GitHub::default().reconcile(&map)?;
                    let tracker_dir = dir.join("tracker");
                    std::fs::create_dir_all(&tracker_dir)?;
                    store::atomic_json(&tracker_dir.join("frontier.json"), &frontier)?;
                    Ok(())
                });
                let suspension = match result {
                    Ok(()) => {
                        failures = 0;
                        "GitHub tracker reconciled; worker dispatch not implemented in this release"
                            .to_owned()
                    }
                    Err(error) => {
                        failures = failures.saturating_add(1);
                        format!("{error:#}")
                    }
                };
                if state.suspension != suspension || state.reconciled {
                    state.suspension = suspension;
                    state.reconciled = false;
                    store::atomic_json(&dir.join("state.json"), &state)?;
                }
                next_check = Instant::now() + Duration::from_secs(check_delay(&state, failures));
            }
        }
        if once {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn check_delay(state: &State, failures: u32) -> u64 {
    state
        .poll_seconds
        .saturating_mul(1u64 << failures.min(4))
        .min(300.max(state.poll_seconds))
}
