use crate::{config::Config, protocol};

#[inline(never)]
pub fn main() {
    // Handle metadata without constructing an async runtime or touching user state.
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--version"] {
        println!("piko {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if args == ["--help"] || args == ["-h"] {
        println!(
            "piko [--acp] [--config PATH]\npiko login|logout [--config PATH]\n\nSilent ACP agent on stdin/stdout. ChatGPT subscription or provider API key.\nNo arguments starts ACP. Only explicit login/help/version print human text."
        );
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(
            std::thread::available_parallelism().map_or(8, |n| n.get().clamp(4, 32)),
        )
        .build()
        .expect("Could not create runtime");
    runtime.block_on(run(args));
    // Tokio's stdin reader cannot interrupt an OS read when the client keeps the pipe open.
    runtime.shutdown_timeout(std::time::Duration::from_millis(250));
}

async fn run(args: Vec<String>) {
    let mut path = None;
    let mut action = "acp";
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--acp" => {}
            "login" | "logout" if action == "acp" => action = &args[index],
            "--config" if index + 1 < args.len() => {
                index += 1;
                path = Some(std::path::PathBuf::from(&args[index]));
            }
            _ => {
                eprintln!("Invalid arguments; use --help.");
                std::process::exit(2);
            }
        }
        index += 1;
    }
    let result = async {
        let config = Config::load(path.as_deref()).await?;
        match action {
            "login" => crate::auth::Auth::login(&config).await,
            "logout" => crate::auth::Auth::logout(&config).await,
            _ => protocol::serve(config).await,
        }
    }
    .await;
    if let Err(error) = result {
        // Fatal startup/transport failures are the only unsolicited diagnostics.
        eprintln!("piko: {error:#}");
        std::process::exit(1);
    }
}
