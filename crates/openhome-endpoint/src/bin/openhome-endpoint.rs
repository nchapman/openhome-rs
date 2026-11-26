//! Runnable OpenHome Media + Songcast audio endpoint daemon.
//!
//! Usage:
//! ```sh
//! cargo run -p openhome-endpoint -- --room "Living Room" --name "OpenHome Endpoint"
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use openhome_endpoint::{AudioSink, CpalSink, EndpointConfig, NullSink, OpenHomeEndpoint};
use openhome_server::SsdpConfig;

#[derive(Debug, PartialEq, Eq)]
struct CliArgs {
    room: String,
    name: String,
    udn: Option<String>,
    bind_addr: SocketAddr,
    advertise_host: Option<String>,
    songcast_interface: Ipv4Addr,
    device: String,
    disable_ssdp: bool,
    disable_sender: bool,
    help: bool,
}

impl CliArgs {
    fn parse_from<I, S>(iter: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut out = Self {
            room: "Living Room".into(),
            name: "OpenHome Endpoint".into(),
            udn: None,
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            advertise_host: None,
            songcast_interface: Ipv4Addr::UNSPECIFIED,
            device: "default".into(),
            disable_ssdp: false,
            disable_sender: false,
            help: false,
        };

        let mut args = iter.into_iter().map(Into::into);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--room" => out.room = args.next().ok_or("--room requires a value")?,
                "--name" => out.name = args.next().ok_or("--name requires a value")?,
                "--udn" => out.udn = Some(args.next().ok_or("--udn requires a value")?),
                "--bind" => {
                    let raw = args.next().ok_or("--bind requires ip:port")?;
                    out.bind_addr = raw.parse().map_err(|e| format!("invalid --bind: {e}"))?;
                }
                "--advertise-host" => {
                    out.advertise_host =
                        Some(args.next().ok_or("--advertise-host requires a value")?);
                }
                "--songcast-interface" => {
                    let raw = args.next().ok_or("--songcast-interface requires an IPv4")?;
                    out.songcast_interface = raw
                        .parse()
                        .map_err(|e| format!("invalid --songcast-interface: {e}"))?;
                }
                "--device" => out.device = args.next().ok_or("--device requires a value")?,
                "--no-ssdp" => out.disable_ssdp = true,
                "--no-sender" => out.disable_sender = true,
                "--help" | "-h" => {
                    out.help = true;
                    return Ok(out);
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }
        Ok(out)
    }

    fn into_config(self) -> EndpointConfig {
        let sink: Arc<dyn AudioSink> = match self.device.as_str() {
            "null" => Arc::new(NullSink::realtime()),
            "default" => Arc::new(CpalSink::default_device()),
            named => Arc::new(CpalSink::named(named)),
        };
        let ssdp = if self.disable_ssdp {
            SsdpConfig::disabled()
        } else {
            SsdpConfig::default()
        };
        EndpointConfig {
            room: self.room,
            name: self.name,
            udn: self.udn,
            bind_addr: self.bind_addr,
            advertise_host: self.advertise_host,
            songcast_interface: self.songcast_interface,
            enable_sender: !self.disable_sender,
            ssdp,
            sink,
            on_error: Some(Arc::new(|msg| {
                eprintln!("openhome-endpoint error: {msg}");
            })),
            ..EndpointConfig::default()
        }
    }
}

async fn run_cli(
    cli: CliArgs,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), Box<dyn std::error::Error>> {
    if cli.help {
        eprintln!(
            "openhome-endpoint [--room NAME] [--name NAME] [--udn UDN] [--bind IP:PORT] [--advertise-host HOST] [--songcast-interface IPV4] [--device default|null|NAME] [--no-ssdp] [--no-sender]"
        );
        return Ok(());
    }

    let room = cli.room.clone();
    let name = cli.name.clone();
    let endpoint = OpenHomeEndpoint::start(cli.into_config()).await?;

    eprintln!(
        "openhome-endpoint listening on {} (room={room:?}, name={name:?}, description={}, sender={:?})",
        endpoint.local_addr(),
        endpoint.description_url(),
        endpoint.sender_uri()
    );

    shutdown.await;
    eprintln!("shutting down...");
    endpoint.shutdown().await;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async {
                if let Some(s) = sigterm.as_mut() {
                    s.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = CliArgs::parse_from(std::env::args().skip(1))?;
    run_cli(cli, shutdown_signal()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_args_parse_flags_help_and_errors() {
        let cli = CliArgs::parse_from([
            "--room",
            "Kitchen",
            "--name",
            "HiFi",
            "--udn",
            "uuid:12345678-1234-1234-1234-123456789abc",
            "--bind",
            "127.0.0.1:49152",
            "--advertise-host",
            "127.0.0.1",
            "--songcast-interface",
            "127.0.0.1",
            "--device",
            "null",
            "--no-ssdp",
            "--no-sender",
        ])
        .unwrap();
        assert_eq!(cli.room, "Kitchen");
        assert_eq!(cli.name, "HiFi");
        assert_eq!(
            cli.udn.as_deref(),
            Some("uuid:12345678-1234-1234-1234-123456789abc")
        );
        assert_eq!(cli.bind_addr, "127.0.0.1:49152".parse().unwrap());
        assert_eq!(cli.advertise_host.as_deref(), Some("127.0.0.1"));
        assert_eq!(cli.songcast_interface, Ipv4Addr::LOCALHOST);
        assert!(cli.disable_ssdp);
        assert!(cli.disable_sender);
        let cfg = cli.into_config();
        assert_eq!(cfg.room, "Kitchen");
        assert!(!cfg.enable_sender);
        (cfg.on_error.as_ref().expect("on_error"))("test diagnostic");

        let default_cfg = CliArgs::parse_from(["--device", "default"])
            .unwrap()
            .into_config();
        assert_eq!(default_cfg.room, "Living Room");
        assert_eq!(default_cfg.sink.name(), "cpal:default");
        assert!(default_cfg.enable_sender);

        let named_cfg = CliArgs::parse_from(["--device", "DAC"])
            .unwrap()
            .into_config();
        assert_eq!(named_cfg.sink.name(), "DAC");

        assert!(CliArgs::parse_from(["--help"]).unwrap().help);
        assert!(CliArgs::parse_from(["--unknown"]).is_err());
        assert!(CliArgs::parse_from(["--bind", "bad"]).is_err());
        assert!(CliArgs::parse_from(["--songcast-interface", "not-an-ip"]).is_err());
    }

    #[tokio::test]
    async fn run_cli_handles_help_and_clean_startup_shutdown() {
        let help_cli = CliArgs::parse_from(["--help"]).unwrap();
        run_cli(help_cli, std::future::ready(())).await.unwrap();

        let cli = CliArgs::parse_from([
            "--room",
            "Test Room",
            "--name",
            "CLI Test",
            "--bind",
            "127.0.0.1:0",
            "--device",
            "null",
            "--no-ssdp",
        ])
        .unwrap();
        run_cli(cli, std::future::ready(())).await.unwrap();
    }
}
