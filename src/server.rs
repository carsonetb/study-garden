use rand::{RngExt, rng};
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use tokio::sync::broadcast::{Receiver, Sender, channel};

use bevy::prelude::*;
use bincode_next::{Decode, Encode};

use crate::network::*;

#[derive(Debug, Deref)]
pub struct ClonableTcpStream(TcpStream);

impl Clone for ClonableTcpStream {
    fn clone(&self) -> Self {
        ClonableTcpStream(self.try_clone().unwrap())
    }
}

#[derive(Resource, Debug)]
pub struct Server<S, C> {
    listener: TcpListener,
    /// Send any message to any client from any thread.
    pub sender: Sender<SendMessage<S>>,
    pub thread_sender: Sender<ReceiveMessage<C>>,
    /// Receive any message from any client from any thread.
    pub receiver: Receiver<ReceiveMessage<C>>,
    intercom_sender: Sender<IntercomMessage>,
    thread_intercom_sender: Sender<IntercomResponse>,
    intercom_receiver: Receiver<IntercomResponse>,
    pub clients: Vec<ClientID>,
    // send: VecDeque<(ClientID, S)>,
    // received: VecDeque<(ClientID, C)>,
}

#[derive(Debug, Clone)]
enum IntercomMessage {
    Connection {
        id: ClientID,
        stream: ClonableTcpStream,
    },
}

#[derive(Debug, Clone)]
enum IntercomResponse {
    Disconnection(ClientID),
}

pub fn server_setup<S, C>(mut commands: Commands)
where
    S: Encode + Clone + Send + Sync + 'static,
    C: Decode<()> + Clone + Send + Sync + 'static,
{
    let listener =
        TcpListener::bind("127.0.0.1:8000").expect("Could not create TCP listener for server.");

    // let Ok(write_stream) = TcpStream::connect("127.0.0.1:8000") else {
    //     error!(
    //         "Failed to create a connection to the address 127.0.0.1:8080, the server will not be created."
    //     );
    //     return;
    // };
    // let read_stream = write_stream
    //     .try_clone()
    //     .expect("Could not clone stream to address 127.0.0.1:8080.");

    let (sender, thread_receiver) = channel::<SendMessage<S>>(16);
    let (thread_sender, receiver) = channel::<ReceiveMessage<C>>(16);
    let (intercom_sender, thread_intercom_receiver) = channel::<IntercomMessage>(16);
    let (thread_intercom_sender, intercom_receiver) = channel::<IntercomResponse>(16);

    let thread_intercom_sender2 = thread_intercom_sender.clone();
    let thread_intercom_sender3 = thread_intercom_sender.clone();
    let thread_intercom_receiver2 = intercom_sender.subscribe();
    let thread_sender2 = thread_sender.clone();
    thread::spawn(move || {
        server_listen(
            thread_sender2,
            thread_intercom_sender3,
            thread_intercom_receiver2,
            Vec::new(),
        )
    });
    thread::spawn(move || {
        server_send(
            thread_receiver,
            thread_intercom_sender2,
            thread_intercom_receiver,
            Vec::new(),
        )
    });

    commands.spawn(Server {
        sender,
        receiver,
        listener,
        thread_sender,
        intercom_sender,
        thread_intercom_sender,
        intercom_receiver,
        clients: Vec::new(),
    });

    info!("Finished server setup.");
}

pub fn server_update<S, C>(
    mut server: ResMut<Server<S, C>>,
    mut network_mw: MessageWriter<NetworkMessage>,
    mut received_mw: MessageWriter<ReceiveMessage<C>>,
    mut send_mr: MessageReader<SendMessage<S>>,
) where
    S: Encode + Clone + Send + Sync + 'static,
    C: Decode<()> + Clone + Send + Sync + 'static,
{
    while let Ok(message) = server.receiver.try_recv() {
        received_mw.write(message);
    }

    for message in send_mr.read() {
        if let Err(err) = server.sender.send(message.clone()) {
            error!(
                "Failed to send a message to the sender thread, connection may have been severed. Error: {err}"
            );
            continue;
        }
    }

    let mut ids = Vec::new();
    server.listener.set_nonblocking(true).unwrap();
    for stream in server.listener.incoming() {
        if let Ok(stream) = stream {
            let id = ClientID(rng().random());
            info!("Connection received from client {id:?}.");

            let stream = ClonableTcpStream(stream);
            server
                .intercom_sender
                .send(IntercomMessage::Connection { id, stream })
                .unwrap();

            ids.push(id);
            network_mw.write(NetworkMessage::ClientConnect(id));
        } else {
            break;
        }
    }
    server.clients.append(&mut ids);

    while let Ok(message) = server.intercom_receiver.try_recv() {
        match message {
            IntercomResponse::Disconnection(id) => {
                info!("Finalizing disconnect for client {id:?}.");
                network_mw.write(NetworkMessage::ClientDisconnect(id));
                server.clients.retain(|other| other != &id);
            }
        }
    }
}

fn server_listen<C>(
    tx: Sender<ReceiveMessage<C>>,
    itx: Sender<IntercomResponse>,
    mut irx: Receiver<IntercomMessage>,
    mut streams: Vec<(ClientID, TcpStream)>,
) where
    C: Decode<()> + Send + Sync + 'static,
{
    loop {
        while let Ok(IntercomMessage::Connection { id, stream }) = irx.try_recv() {
            info!("Connection registered on listen thread.");
            streams.push((id, stream.0));
        }

        let mut remove = Vec::new();
        for (id, stream) in &mut streams {
            let mut len_buffer = [0u8; 4];
            if let Err(err) = stream.read_exact(&mut len_buffer) {
                if err.kind() == io::ErrorKind::WouldBlock {
                    continue;
                }

                error!(
                    "Failed to read length of next message in TCP stream for client {id:?}. Error: {err}"
                );
                warn!("Client disconnect detected (listen thread).");
                remove.push(*id);
                itx.send(IntercomResponse::Disconnection(*id)).unwrap();
                continue;
            }
            let len = u32::from_be_bytes(len_buffer) as usize;

            let mut body = vec![0u8; len];
            if let Err(err) = stream.read_exact(&mut body) {
                error!(
                    "Failed to read next message in TCP stream (although reading the length was successful, it was {len}) for client {id:?}. Error: {err}"
                );
                continue;
            }
            let message: C =
                match bincode_next::decode_from_slice(&body, bincode_next::config::standard()) {
                    Result::Ok((message, _)) => message,
                    Result::Err(err) => {
                        error!("Failed to read from TCP stream for client {id:?}. Error: {err:?}");
                        continue;
                    }
                };

            if let Err(err) = tx.send(ReceiveMessage { id: *id, message }) {
                error!(
                    "Server listen thread has become disconnected, and will be shutdown. Error: {err}"
                );
                return;
            }
        }

        for id in remove {
            streams.retain(|(compare, _)| &id != compare);
        }
    }
}

fn server_send<S>(
    mut rx: Receiver<SendMessage<S>>,
    mut itx: Sender<IntercomResponse>,
    mut irx: Receiver<IntercomMessage>,
    streams: Vec<(ClientID, TcpStream)>,
) where
    S: Encode + Clone + Send + Sync + 'static,
{
    let mut streams: HashMap<ClientID, TcpStream> = streams.into_iter().collect();
    let mut queue = VecDeque::new();
    loop {
        while let Ok(IntercomMessage::Connection { id, stream }) = irx.try_recv() {
            info!("Connection registered on send thread.");
            streams.insert(id, stream.0);
        }

        while let Ok(message) = rx.try_recv() {
            queue.push_back(message);
        }

        while let Some(message) = queue.pop_front() {
            let Some(stream) = streams.get_mut(&message.id) else {
                error!(
                    "Tried to send message to client (ID {:?}) which is not in the TCP Stream database.",
                    message.id
                );
                continue;
            };

            let bytes =
                bincode_next::encode_to_vec(message.message, bincode_next::config::standard())
                    .expect("Could not encode a message!");
            let len = (bytes.len() as u32).to_be_bytes();

            if let Err(err) = stream.write_all(&len) {
                error!("Failed to write length of message to TCP stream. Error: {err}");
                warn!("Client disconnect detected (send thread).");
                streams.remove(&message.id);
                continue;
            }
            if let Err(err) = stream.write_all(&bytes) {
                error!("Failed to write message to TCP stream. Error: {err}");
                warn!("Client disconnect detected (send thread).");
                streams.remove(&message.id);
                continue;
            }
            if let Err(err) = stream.flush() {
                error!("Failed to flush TCP stream. Error: {err}");
                continue;
            }
        }
    }
}
