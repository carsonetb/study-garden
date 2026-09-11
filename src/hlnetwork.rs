use bevy::{prelude::*, tasks::AsyncComputeTaskPool};
use bevy_defer::*;
use bincode_next::{Decode, Encode};
use crossbeam_channel::{Receiver, Sender, unbounded};

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

#[derive(Resource, Clone)]
struct ServerAsyncEventsMain {
    tx: Sender<ClientMessage>,
    rx: Receiver<ClientMessage>,
}

#[derive(Resource, Clone)]
struct ServerAsyncEventsThread {
    tx: Sender<ClientMessage>,
    rx: Receiver<ClientMessage>,
}

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

        let (tx_thread, rx_main) = unbounded::<ClientMessage>();
        let (tx_main, rx_thread) = unbounded::<ClientMessage>();
        app.insert_resource(ServerAsyncEventsMain {
            tx: tx_main,
            rx: rx_main,
        });
        app.insert_resource(ServerAsyncEventsThread {
            tx: tx_thread,
            rx: rx_thread,
        });
    }
}

fn hlserver_update(
    mut network_mr: MessageReader<NetworkMessage>,
    mut sender: MessageWriter<SendMessage<ServerMessage>>,
    mut receiver: MessageReader<ReceiveMessage<ClientMessage>>,
    events: Res<ServerAsyncEventsMain>,
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

    for message in receiver.read() {
        events.tx.send(message.message.clone());
    }

    while let Ok(message) = events.rx.try_recv() {
        sender.write(SendMessage)
    }
}

fn server_ping(rx: Res<ServerAsyncEvents>, server: Res<Server<ServerMessage, ClientMessage>>) {
    let clients = server.clients.clone();
    let rx = rx.0.clone();

    AsyncComputeTaskPool::get().spawn(async move {}).detach();

    // commands.spawn_task(async move || {
    //     let clients = AsyncWorld
    //         .resource::<Server<ServerMessage, ClientMessage>>()
    //         .with(|server| server.clients.clone());

    //     for client in clients {
    //         AsyncWorld
    //             .write_message(SendMessage::new(client, ServerMessage::Ping))
    //             .unwrap();
    //     }

    //     let mut reader = AsyncWorld.next

    //     Ok(())
    // });
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
                info!("Setup received from server, this client has ID {id:?}");
                client.id = Some(id);
            }
            &ServerMessage::Ping => {
                sender.write(SendMessage::server(ClientMessage::Pong));
            }
        }
    }
}
