use bevy::reflect::list::List;
use crossbeam_channel::{Receiver, Sender, unbounded};
use rand::{RngExt, rng};
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::net::{TcpListener, TcpStream};
use std::thread;

use bevy::prelude::*;
use bincode_next::{Decode, Encode};

#[derive(
    Encode, Decode, Deref, Debug, Default, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct ClientID(u64);

#[derive(Message)]
pub enum NetworkMessage {
    ClientConnect(ClientID),
    ClientDisconnect(ClientID),
}

#[derive(Message, Clone)]
pub struct SendMessage<T> {
    pub id: ClientID,
    pub message: T,
}

impl<T> SendMessage<T> {
    pub fn new(id: ClientID, message: T) -> Self {
        Self { id, message }
    }

    pub fn server(message: T) -> Self {
        SendMessage {
            id: ClientID(0),
            message,
        }
    }
}

#[derive(Message, Clone)]
pub struct ReceiveMessage<T> {
    pub id: ClientID,
    pub message: T,
}

#[derive(Debug)]
pub struct Connection {
    id: ClientID,
    stream: TcpStream,
}

#[derive(Resource, Debug)]
pub struct Server<S, C> {
    sender: Sender<SendMessage<S>>,
    receiver: Receiver<ReceiveMessage<C>>,
    rc_sender: Sender<Connection>,
    sc_sender: Sender<Connection>,
    listener: TcpListener,
    // send: VecDeque<(ClientID, S)>,
    // received: VecDeque<(ClientID, C)>,
}

pub fn server_setup<S, C>(mut commands: Commands)
where
    S: Encode + Send + Sync + 'static,
    C: Decode<()> + Send + Sync + 'static,
{
    let listener =
        TcpListener::bind("127.0.0.1:8000").expect("Could not create TCP listener for server.");
    listener.set_nonblocking(true).unwrap();

    // let Ok(write_stream) = TcpStream::connect("127.0.0.1:8000") else {
    //     error!(
    //         "Failed to create a connection to the address 127.0.0.1:8080, the server will not be created."
    //     );
    //     return;
    // };
    // let read_stream = write_stream
    //     .try_clone()
    //     .expect("Could not clone stream to address 127.0.0.1:8080.");

    let (sender, thread_receiver) = unbounded::<SendMessage<S>>();
    let (thread_sender, receiver) = unbounded::<ReceiveMessage<C>>();
    let (listen_connection_sender, listen_connection_receiver) = unbounded::<Connection>();
    let (send_connection_sender, send_connection_receiver) = unbounded::<Connection>();

    thread::spawn(move || server_listen(thread_sender, listen_connection_receiver, Vec::new()));
    thread::spawn(move || server_send(thread_receiver, send_connection_receiver, Vec::new()));

    commands.spawn(Server {
        sender,
        receiver,
        listener,
        rc_sender: listen_connection_sender,
        sc_sender: send_connection_sender,
    });

    info!("Finished server setup.");
}

pub fn server_update<S, C>(
    server: ResMut<Server<S, C>>,
    mut network_mw: MessageWriter<NetworkMessage>,
    mut received_mw: MessageWriter<ReceiveMessage<C>>,
    mut send_mr: MessageReader<SendMessage<S>>,
) where
    S: Encode + Clone + Send + Sync + 'static,
    C: Decode<()> + Send + Sync + 'static,
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

    for stream in server.listener.incoming() {
        if let Ok(stream) = stream {
            let id = ClientID(rng().random());
            info!("Connection received from client.");

            let stream2 = stream.try_clone().expect("Could not clone stream.");
            if let Err(err) = server.sc_sender.send(Connection { id, stream }) {
                error!(
                    "Could not send message to server sender thread, it might have been closed. Error: {err}"
                );
                break;
            }
            if let Err(err) = server.rc_sender.send(Connection {
                id,
                stream: stream2,
            }) {
                error!(
                    "Could not send message to server receiver thread, it might have been closed. Error: {err}"
                );
                break;
            }

            network_mw.write(NetworkMessage::ClientConnect(id));
        } else {
            break;
        }
    }
}

pub fn server_listen<C>(
    tx: Sender<ReceiveMessage<C>>,
    crx: Receiver<Connection>,
    mut streams: Vec<(ClientID, TcpStream)>,
) where
    C: Decode<()> + Send + Sync + 'static,
{
    loop {
        while let Ok(connection) = crx.try_recv() {
            info!("Connection registered on listen thread.");
            streams.push((connection.id, connection.stream));
        }

        for (id, stream) in &mut streams {
            let mut len_buffer = [0u8; 4];
            if let Err(err) = stream.read_exact(&mut len_buffer) {
                error!(
                    "Failed to read length of next message in TCP stream for client {id:?}. Error: {err}"
                );
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
    }
}

pub fn server_send<S>(
    rx: Receiver<SendMessage<S>>,
    crx: Receiver<Connection>,
    streams: Vec<(ClientID, TcpStream)>,
) where
    S: Encode + Send + Sync + 'static,
{
    let mut streams: HashMap<ClientID, TcpStream> = streams.into_iter().collect();
    let mut queue = VecDeque::new();
    loop {
        while let Ok(connection) = crx.try_recv() {
            info!(
                "Connection to ID {:?} registered on send thread.",
                connection.id
            );
            streams.insert(connection.id, connection.stream);
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
                // queue.push_back(message);
                continue;
            };

            let bytes =
                bincode_next::encode_to_vec(message.message, bincode_next::config::standard())
                    .expect("Could not encode a message!");
            let len = (bytes.len() as u32).to_be_bytes();

            if let Err(err) = stream.write_all(&len) {
                error!("Failed to write length of message to TCP stream. Error: {err}");
                continue;
            }
            if let Err(err) = stream.write_all(&bytes) {
                error!("Failed to write message to TCP stream. Error: {err}");
                continue;
            }
            if let Err(err) = stream.flush() {
                error!("Failed to flush TCP stream. Error: {err}");
                continue;
            }
        }
    }
}

#[derive(Resource, Debug)]
pub struct Client<S, C> {
    pub id: Option<ClientID>,
    sender: Sender<SendMessage<C>>,
    receiver: Receiver<ReceiveMessage<S>>,
    // send: VecDeque<C>,
    // received: VecDeque<S>,
}

impl<S, C> Client<S, C> {
    pub fn message(&self, message: C) -> SendMessage<C> {
        SendMessage::new(
            self.id
                .expect("Identify must be sent before other messages."),
            message,
        )
    }
}

pub fn client_setup<S, C>(mut commands: Commands)
where
    S: Decode<()> + Send + Sync + 'static,
    C: Encode + Send + Sync + 'static,
{
    let Ok(write_stream) = TcpStream::connect("127.0.0.1:8000") else {
        error!(
            "Failed to create a connection to the address 127.0.0.1:8080, the client will not be created."
        );
        return;
    };
    let read_stream = write_stream
        .try_clone()
        .expect("Could not clone stream to address 127.0.0.1:8080.");

    let (sender, thread_receiver) = unbounded::<SendMessage<C>>();
    let (thread_sender, receiver) = unbounded::<ReceiveMessage<S>>();

    thread::spawn(move || client_listen(thread_sender, read_stream));
    thread::spawn(move || client_send(thread_receiver, write_stream));

    commands.spawn(Client {
        id: None,
        sender,
        receiver,
        // send: VecDeque::new(),
        // received: VecDeque::new(),
    });
}

pub fn client_update<S, C>(
    client: Res<Client<S, C>>,
    mut received_mw: MessageWriter<ReceiveMessage<S>>,
    mut send_mr: MessageReader<SendMessage<C>>,
) where
    S: Send + Sync + 'static,
    C: Send + Clone + Sync + 'static,
{
    while let Ok(message) = client.receiver.try_recv() {
        received_mw.write(message);
    }

    for message in send_mr.read() {
        if let Err(err) = client.sender.send(message.clone()) {
            error!(
                "Failed to send a message to the sender thread, connection may have been severed. Error: {err}"
            );
            continue;
        }
    }
}

pub fn client_listen<S>(tx: Sender<ReceiveMessage<S>>, mut stream: TcpStream)
where
    S: Decode<()> + Send + Sync + 'static,
{
    loop {
        let mut len_buffer = [0u8; 4];
        if let Err(err) = stream.read_exact(&mut len_buffer) {
            error!("Failed to read length of next message in TCP stream. Error: {err}");
            continue;
        }
        let len = u32::from_be_bytes(len_buffer) as usize;

        let mut body = vec![0u8; len];
        if let Err(err) = stream.read_exact(&mut body) {
            error!(
                "Failed to read next message in TCP stream (although reading the length was successful, it was {len}). Error: {err}"
            );
            continue;
        }
        let message: S =
            match bincode_next::decode_from_slice(&body, bincode_next::config::standard()) {
                Result::Ok((message, _)) => message,
                Result::Err(err) => {
                    error!("Failed to read from TCP stream. Error: {err:?}");
                    continue;
                }
            };

        if let Err(err) = tx.send(ReceiveMessage {
            id: ClientID(0), // Server is always 0
            message,
        }) {
            error!(
                "Server listen thread has become disconnected, and will be shutdown. Error: {err}"
            );
            return;
        }
    }
}

pub fn client_send<C>(rx: Receiver<SendMessage<C>>, mut stream: TcpStream)
where
    C: Encode + Send + Sync + 'static,
{
    loop {
        while let Ok(message) = rx.try_recv() {
            if message.id != ClientID(0) {
                error!("Client cannot send a message to a client which is not the server.");
                continue;
            };

            let bytes =
                bincode_next::encode_to_vec(message.message, bincode_next::config::standard())
                    .expect("Could not encode a message!");
            let len = (bytes.len() as u32).to_be_bytes();

            if let Err(err) = stream.write_all(&len) {
                error!("Failed to write length of message to TCP stream. Error: {err}");
                continue;
            }
            if let Err(err) = stream.write_all(&bytes) {
                error!("Failed to write message to TCP stream. Error: {err}");
                continue;
            }
            if let Err(err) = stream.flush() {
                error!("Failed to flush TCP stream. Error: {err}");
                continue;
            }
        }
    }
}
