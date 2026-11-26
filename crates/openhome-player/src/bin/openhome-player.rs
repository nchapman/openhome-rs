//! Runnable OpenHome Media + Songcast player daemon.
//!
//! Usage:
//! ```sh
//! cargo run -p openhome-player --features cpal-output -- --room "Living Room" --name "OpenHome Player"
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use openhome_player::{AudioSink, NullSink, OpenHomePlayer, PlayerConfig};
use openhome_server::SsdpConfig;

#[derive(Debug, PartialEq, Eq)]
struct CliArgs {
    room: String,
    name: String,
    bind_addr: SocketAddr,
    advertise_host: Option<String>,
    device: String,
    disable_ssdp: bool,
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
            name: "OpenHome Player".into(),
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            advertise_host: None,
            device: "default".into(),
            disable_ssdp: false,
            help: false,
        };

        let mut args = iter.into_iter().map(Into::into);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--room" => out.room = args.next().ok_or("--room requires a value")?,
                "--name" => out.name = args.next().ok_or("--name requires a value")?,
                "--bind" => {
                    let raw = args.next().ok_or("--bind requires ip:port")?;
                    out.bind_addr = raw.parse().map_err(|e| format!("invalid --bind: {e}"))?;
                }
                "--advertise-host" => {
                    out.advertise_host =
                        Some(args.next().ok_or("--advertise-host requires a value")?);
                }
                "--device" => out.device = args.next().ok_or("--device requires a value")?,
                "--no-ssdp" => out.disable_ssdp = true,
                "--help" | "-h" => {
                    out.help = true;
                    return Ok(out);
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }
        Ok(out)
    }

    fn into_config(self) -> PlayerConfig {
        let sink: Arc<dyn AudioSink> = match self.device.as_str() {
            "null" => Arc::new(NullSink::realtime()),
            _ => default_sink(&self.device),
        };
        let ssdp = if self.disable_ssdp {
            SsdpConfig::disabled()
        } else {
            SsdpConfig::default()
        };
        PlayerConfig {
            room: self.room,
            name: self.name,
            bind_addr: self.bind_addr,
            advertise_host: self.advertise_host,
            ssdp,
            sink,
            ..PlayerConfig::default()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = CliArgs::parse_from(std::env::args().skip(1))?;
    if cli.help {
        eprintln!(
            "openhome-player [--room NAME] [--name NAME] [--bind IP:PORT] [--advertise-host HOST] [--device default|null|NAME] [--no-ssdp]"
        );
        return Ok(());
    }

    let room = cli.room.clone();
    let name = cli.name.clone();
    let player = OpenHomePlayer::start(cli.into_config()).await?;

    eprintln!(
        "openhome-player listening on {} (room={room:?}, name={name:?}, description={})",
        player.local_addr(),
        player.description_url()
    );

    tokio::signal::ctrl_c().await?;
    eprintln!("shutting down...");
    player.shutdown().await;
    Ok(())
}

#[cfg(feature = "cpal-output")]
fn default_sink(device: &str) -> Arc<dyn AudioSink> {
    if device == "default" {
        Arc::new(openhome_player::CpalSink::default_device())
    } else {
        Arc::new(openhome_player::CpalSink::named(device))
    }
}

#[cfg(not(feature = "cpal-output"))]
fn default_sink(_device: &str) -> Arc<dyn AudioSink> {
    eprintln!("note: built without --features cpal-output; falling back to realtime NullSink");
    Arc::new(NullSink::realtime())
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
            "--bind",
            "127.0.0.1:49152",
            "--advertise-host",
            "127.0.0.1",
            "--device",
            "null",
            "--no-ssdp",
        ])
        .unwrap();
        assert_eq!(cli.room, "Kitchen");
        assert_eq!(cli.name, "HiFi");
        assert_eq!(cli.bind_addr, "127.0.0.1:49152".parse().unwrap());
        assert_eq!(cli.advertise_host.as_deref(), Some("127.0.0.1"));
        assert!(cli.disable_ssdp);
        let cfg = cli.into_config();
        assert_eq!(cfg.room, "Kitchen");

        let default_cfg = CliArgs::parse_from(["--device", "default"])
            .unwrap()
            .into_config();
        assert_eq!(default_cfg.room, "Living Room");

        assert!(CliArgs::parse_from(["--help"]).unwrap().help);
        assert!(CliArgs::parse_from(["--unknown"]).is_err());
        assert!(CliArgs::parse_from(["--bind", "bad"]).is_err());
    }
}
