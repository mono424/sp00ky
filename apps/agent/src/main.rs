//! See `lib.rs`. Two invocations:
//!
//! ```text
//! spky-agent -- <backend command...>   supervise a backend on a pool machine
//! spky-agent --install <dest>          copy this binary to <dest> and exit
//! ```

use spky_agent::{install, run, AgentConfig, Exit};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.first().map(String::as_str) == Some("--install") {
        let Some(dest) = args.get(1) else {
            eprintln!("usage: spky-agent --install <dest>");
            std::process::exit(2);
        };
        match install(dest) {
            Ok(()) => return,
            Err(e) => {
                eprintln!("[spky-agent] install failed: {e}");
                std::process::exit(1);
            }
        }
    }

    let command: Vec<String> = match args.iter().position(|a| a == "--") {
        Some(i) => args[i + 1..].to_vec(),
        None => Vec::new(),
    };
    if command.is_empty() {
        eprintln!("usage: spky-agent -- <backend command...>");
        std::process::exit(2);
    }
    let cfg = match AgentConfig::from_env(command) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("[spky-agent] {e}");
            std::process::exit(2);
        }
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let exit = runtime.block_on(run(cfg));
    eprintln!("[spky-agent] exiting: {exit:?}");
    // Every exit is a clean one: the machine's supervisor (docker, the VM's unit)
    // must NOT restart the agent of a machine the scheduler has let go.
    std::process::exit(match exit {
        Exit::Dismissed | Exit::Signalled | Exit::Orphaned => 0,
    });
}
