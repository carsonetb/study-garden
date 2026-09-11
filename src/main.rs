use std::env;

use bevy::prelude::*;
use bevy_defer::AsyncPlugin;

mod hlnetwork;
mod networking;

fn setup(mut commands: Commands) {
    commands.spawn(Camera2d);
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut app = App::new();

    app.add_plugins((DefaultPlugins, AsyncPlugin::default_settings()));
    if args.len() >= 2 && &args[1] == "server" {
        app.add_plugins(hlnetwork::ServerPlugin);
    } else {
        app.add_plugins(hlnetwork::ClientPlugin);
    }

    app.add_systems(Startup, setup);

    app.run();
}
