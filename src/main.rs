use std::env;

use bevy::{log::LogPlugin, prelude::*};
use bevy_defer::AsyncPlugin;

mod client;
mod hlnetwork;
mod network;
mod server;

fn setup(mut commands: Commands) {
    commands.spawn(Camera2d);
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut app = App::new();

    app.add_plugins(AsyncPlugin::default_settings());
    if args.len() >= 2 && &args[1] == "server" {
        app.add_plugins(hlnetwork::ServerPlugin);
        app.add_plugins((MinimalPlugins, LogPlugin::default()));
    } else {
        app.add_plugins(hlnetwork::ClientPlugin);
        app.add_plugins(DefaultPlugins);
        app.add_systems(Startup, setup);
    }

    app.run();
}
