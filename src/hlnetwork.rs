use bevy::prelude::*;
use bincode_next::{Decode, Encode};

use crate::client::*;
use crate::network::*;
use crate::server::*;

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
                // server_ping.run_if(on_timer(Duration::from_secs(5))),
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

// fn server_ping(server: Res<Server<ServerMessage, ClientMessage>>) {
//     let clients = server.clients.clone();
//     let sender = server.sender.clone();
//     let mut receiver = server.thread_sender.subscribe();

//     AsyncComputeTaskPool::get()
//         .spawn(async move {
//             for client in clients {
//                 info!("Ping client {client:?}.");
//                 let _ = sender.send(SendMessage::new(client, ServerMessage::Ping));

//                 let start = Instant::now();
//                 loop {
//                     if start.elapsed() > Duration::from_secs(1) {
//                         warn!("Client disconnect.");
//                         break;
//                     }
//                     let Ok(recv) = receiver.try_recv() else {
//                         continue;
//                     };
//                     if let ClientMessage::Pong = recv.message {
//                         info!("Pong.");
//                         break;
//                     }
//                 }
//             }
//         })
//         .detach();
// }

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
                info!("Setup received from server, this client has ID {id:?}");
                client.id = Some(id);
            }
            &ServerMessage::Ping => {
                info!("Ping received from server, responding.");
                sender.write(SendMessage::server(ClientMessage::Pong));
            }
        }
    }
}
