use bevy::prelude::*;
use bincode_next::{Decode, Encode};

use crate::networking::*;

#[derive(Debug, Clone, Encode, Decode)]
pub enum ServerMessage {
    Ping,
    Identify(ClientID),
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum ClientMessage {
    Pong,
}

#[derive(Debug, Clone, Copy)]
pub struct ServerPlugin;

impl Plugin for ServerPlugin {
    fn build(&self, app: &mut App) {
        info!("Loading server plugin.");

        app.add_message::<NetworkMessage>();
        app.add_message::<SendMessage<ServerMessage>>();
        app.add_message::<ReceiveMessage<ClientMessage>>();
        app.add_systems(Startup, server_setup::<ServerMessage, ClientMessage>);
        app.add_systems(
            FixedUpdate,
            (
                server_update::<ServerMessage, ClientMessage>,
                hlserver_update,
            ),
        );
    }
}

fn hlserver_update(
    mut network_mr: MessageReader<NetworkMessage>,
    mut sender: MessageWriter<SendMessage<ServerMessage>>,
) {
    for message in network_mr.read() {
        match message {
            &NetworkMessage::ClientConnect(id) => {
                info!("Identifying client {id:?}");
                sender.write(SendMessage::new(id, ServerMessage::Identify(id)));
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ClientPlugin;

impl Plugin for ClientPlugin {
    fn build(&self, app: &mut App) {
        info!("Loading client plugin.");

        app.add_message::<NetworkMessage>();
        app.add_message::<ReceiveMessage<ServerMessage>>();
        app.add_message::<SendMessage<ClientMessage>>();
        app.add_systems(Startup, client_setup::<ServerMessage, ClientMessage>);
        app.add_systems(
            FixedUpdate,
            (
                client_update::<ServerMessage, ClientMessage>,
                hlclient_update,
            ),
        );
    }
}

fn hlclient_update(
    mut receiver: MessageReader<ReceiveMessage<ServerMessage>>,
    mut sender: MessageWriter<SendMessage<ClientMessage>>,
    mut client: ResMut<Client<ServerMessage, ClientMessage>>,
) {
    for message in receiver.read() {
        match &message.message {
            &ServerMessage::Identify(id) => {
                info!("This client has ID {id:?}");
                client.id = Some(id);
            }
            &ServerMessage::Ping => {
                sender.write(SendMessage::server(ClientMessage::Pong));
            }
        }
    }
}
