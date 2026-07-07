//! Tokio ベースの MQTT v5.0 クライアント example。
//!
//! `shiguredo_mqtt` は Sans-I/O のため、TCP / TLS 接続とイベントループは
//! この example 側で tokio を使って実装する。
//!
//! 使い方:
//! ```text
//! # メッセージを公開する
//! cargo run -p tokio-mqtt -- publish --topic demo/hello --message 'hi'
//!
//! # トピックを購読する（Ctrl+C で終了）
//! cargo run -p tokio-mqtt -- subscribe --topic demo/hello
//!
//! # mqtts (TCP + TLS)
//! cargo run -p tokio-mqtt -- publish \
//!   --port 8883 --ca-file /path/to/ca.pem --server-name localhost \
//!   --topic demo/hello --message 'hi'
//! ```

use std::path::PathBuf;

use shiguredo_mqtt::codec::qos::QoS;
use tokio_mqtt::client::MqttClient;
use tokio_mqtt::error::Error;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// CLI から受け取った実行コマンド。
enum Command {
    /// トピックへ 1 通公開して終了する。
    Publish(PublishOptions),
    /// トピックを購読し続ける。
    Subscribe(SubscribeOptions),
}

struct CommonOptions {
    host: String,
    port: u16,
    /// 指定時は mqtts (TCP + TLS)。省略時は平文 TCP。
    ca_file: Option<PathBuf>,
    server_name: String,
    client_id: String,
    keep_alive: u16,
    verbose: bool,
}

struct PublishOptions {
    common: CommonOptions,
    topic: String,
    message: String,
    qos: QoS,
    retain: bool,
}

struct SubscribeOptions {
    common: CommonOptions,
    topic: String,
    qos: QoS,
}

#[tokio::main]
async fn main() -> noargs::Result<()> {
    let command = parse_args()?;
    init_tracing(match &command {
        Command::Publish(opts) => opts.common.verbose,
        Command::Subscribe(opts) => opts.common.verbose,
    });

    match command {
        Command::Publish(opts) => run_publish(opts).await?,
        Command::Subscribe(opts) => run_subscribe(opts).await?,
    }
    Ok(())
}

fn init_tracing(verbose: bool) {
    let default = if verbose {
        "tokio_mqtt=debug"
    } else {
        "tokio_mqtt=info"
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

async fn run_publish(opts: PublishOptions) -> Result<(), Error> {
    let mut client = open_client(&opts.common).await?;
    client
        .connect(&opts.common.client_id, opts.common.keep_alive, true)
        .await?;
    client
        .publish(&opts.topic, opts.message.as_bytes(), opts.qos, opts.retain)
        .await?;
    client.disconnect().await?;
    Ok(())
}

async fn run_subscribe(opts: SubscribeOptions) -> Result<(), Error> {
    let mut client = open_client(&opts.common).await?;
    client
        .connect(&opts.common.client_id, opts.common.keep_alive, true)
        .await?;
    client.subscribe(&opts.topic, opts.qos).await?;
    let result = client.run_subscribe_loop().await;
    // ループ終了後は可能な範囲で DISCONNECT を送る。
    if let Err(e) = client.disconnect().await {
        info!(error = %e, "disconnect after subscribe loop failed");
    }
    result
}

/// `--ca-file` の有無で平文 TCP / mqtts を切り替える。
async fn open_client(opts: &CommonOptions) -> Result<MqttClient, Error> {
    match &opts.ca_file {
        Some(ca_file) => {
            let ca_pem = std::fs::read_to_string(ca_file)?;
            MqttClient::connect_tls(&opts.host, opts.port, &ca_pem, &opts.server_name).await
        }
        None => MqttClient::connect_tcp(&opts.host, opts.port).await,
    }
}

fn parse_args() -> noargs::Result<Command> {
    let mut args = noargs::raw_args();
    args.metadata_mut().app_name = env!("CARGO_PKG_NAME");
    args.metadata_mut().app_description = env!("CARGO_PKG_DESCRIPTION");

    if noargs::VERSION_FLAG.take(&mut args).is_present() {
        println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        std::process::exit(0);
    }
    noargs::HELP_FLAG.take_help(&mut args);

    if noargs::cmd("publish")
        .doc("Publish a message to a topic")
        .take(&mut args)
        .is_present()
    {
        let opts = parse_publish_options(&mut args)?;
        finish_or_help(args)?;
        return Ok(Command::Publish(opts));
    }

    if noargs::cmd("subscribe")
        .doc("Subscribe to a topic")
        .take(&mut args)
        .is_present()
    {
        let opts = parse_subscribe_options(&mut args)?;
        finish_or_help(args)?;
        return Ok(Command::Subscribe(opts));
    }

    // サブコマンド未指定時はヘルプを出す。
    finish_or_help(args)?;
    Err("subcommand `publish` or `subscribe` is required".into())
}

fn finish_or_help(args: noargs::RawArgs) -> noargs::Result<()> {
    if let Some(help) = args.finish()? {
        print!("{help}");
        std::process::exit(0);
    }
    Ok(())
}

fn parse_publish_options(args: &mut noargs::RawArgs) -> noargs::Result<PublishOptions> {
    let common = parse_common_options(args)?;
    let topic: String = noargs::opt("topic")
        .short('t')
        .ty("TOPIC")
        .doc("Topic to publish to")
        .example("demo/hello")
        .take(args)
        .then(|o| Ok::<_, std::convert::Infallible>(o.value().to_string()))?;
    let message: String = noargs::opt("message")
        .short('m')
        .ty("TEXT")
        .doc("Message payload to publish")
        .example("hello")
        .take(args)
        .then(|o| Ok::<_, std::convert::Infallible>(o.value().to_string()))?;
    let qos = parse_qos_opt(args)?;
    let retain = noargs::flag("retain")
        .doc("Set the RETAIN flag")
        .take(args)
        .is_present();

    Ok(PublishOptions {
        common,
        topic,
        message,
        qos,
        retain,
    })
}

fn parse_subscribe_options(args: &mut noargs::RawArgs) -> noargs::Result<SubscribeOptions> {
    let common = parse_common_options(args)?;
    let topic: String = noargs::opt("topic")
        .short('t')
        .ty("TOPIC")
        .doc("Topic filter to subscribe to")
        .example("demo/hello")
        .take(args)
        .then(|o| Ok::<_, std::convert::Infallible>(o.value().to_string()))?;
    let qos = parse_qos_opt(args)?;

    Ok(SubscribeOptions { common, topic, qos })
}

fn parse_common_options(args: &mut noargs::RawArgs) -> noargs::Result<CommonOptions> {
    // `--host` に短オプション `-h` は付けない（`--help` / `-h` と衝突するため）。
    let host: String = noargs::opt("host")
        .ty("HOST")
        .doc("Broker host name")
        .default("127.0.0.1")
        .take(args)
        .then(|o| Ok::<_, std::convert::Infallible>(o.value().to_string()))?;
    let port: u16 = noargs::opt("port")
        .short('p')
        .ty("PORT")
        .doc("Broker port number (1883 plain / 8883 mqtts typical)")
        .default("1883")
        .take(args)
        .then(|o| o.value().parse())?;
    let ca_file: Option<PathBuf> = noargs::opt("ca-file")
        .ty("PATH")
        .doc("CA certificate PEM for mqtts (TCP + TLS); omit for plain TCP")
        .example("/path/to/ca.pem")
        .take(args)
        .present_and_then(|o| Ok::<_, std::convert::Infallible>(PathBuf::from(o.value())))?;
    let server_name: String = noargs::opt("server-name")
        .ty("NAME")
        .doc("TLS server name (SNI / certificate verification); used with --ca-file")
        .default("localhost")
        .take(args)
        .then(|o| Ok::<_, std::convert::Infallible>(o.value().to_string()))?;
    let client_id: String = noargs::opt("client-id")
        .ty("ID")
        .doc("MQTT Client Identifier")
        .default("tokio-mqtt")
        .take(args)
        .then(|o| Ok::<_, std::convert::Infallible>(o.value().to_string()))?;
    let keep_alive: u16 = noargs::opt("keep-alive")
        .ty("SECS")
        .doc("Keep Alive interval in seconds")
        .default("60")
        .take(args)
        .then(|o| o.value().parse())?;
    let verbose = noargs::flag("verbose")
        .short('v')
        .doc("Enable debug logging")
        .take(args)
        .is_present();

    Ok(CommonOptions {
        host,
        port,
        ca_file,
        server_name,
        client_id,
        keep_alive,
        verbose,
    })
}

fn parse_qos_opt(args: &mut noargs::RawArgs) -> noargs::Result<QoS> {
    noargs::opt("qos")
        .short('q')
        .ty("0|1|2")
        .doc("QoS level")
        .default("0")
        .take(args)
        .then(|o| -> Result<QoS, String> {
            let value: u8 = o
                .value()
                .parse()
                .map_err(|e: std::num::ParseIntError| e.to_string())?;
            QoS::from_u8(value).ok_or_else(|| format!("invalid QoS: {value} (expected 0, 1, or 2)"))
        })
}
