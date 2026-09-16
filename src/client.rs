use rand::{RngExt, rng};
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use tokio::sync::broadcast::{Receiver, Sender, channel};

use bevy::prelude::*;
use bincode_next::{Decode, Encode};

use crate::network::*;

#[derive(Resource, Debug)]
pub struct Client<S, C> {
    pub id: Option<ClientID>,
    pub sender: Sender<SendMessage<C>>,
    pub thread_sender: Sender<ReceiveMessage<S>>,
    pub receiver: Receiver<ReceiveMessage<S>>,
    intercom_sender: Sender<IntercomMessage>,
    thread_intercom_sender: Sender<IntercomResponse>,
    intercom_receiver: Receiver<IntercomResponse>,
    pub disconnected: bool,
}

#[derive(Debug, Clone)]
enum IntercomMessage {}

#[derive(Debug, Clone)]
enum IntercomResponse {
    ServerDisconnect,
    UnexpectedError,
}

pub fn client_setup<S, C>(mut commands: Commands)
where
    S: Decode<()> + Clone + Send + Sync + 'static,
    C: Encode + Clone + Send + Sync + 'static,
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

    let (sender, thread_receiver) = channel::<SendMessage<C>>(16);
    let (thread_sender, receiver) = channel::<ReceiveMessage<S>>(16);
    let (intercom_sender, thread_intercom_receiver) = channel::<IntercomMessage>(16);
    let (thread_intercom_sender, intercom_receiver) = channel::<IntercomResponse>(16);

    let thread_intercom_sender2 = thread_intercom_sender.clone();
    let thread_intercom_sender3 = thread_intercom_sender.clone();
    let thread_intercom_receiver2 = intercom_sender.subscribe();
    let thread_sender2 = thread_sender.clone();
    thread::spawn(move || {
        client_listen(
            thread_sender2,
            thread_intercom_sender2,
            thread_intercom_receiver2,
            read_stream,
        )
    });
    thread::spawn(move || {
        client_send(
            thread_receiver,
            thread_intercom_sender3,
            thread_intercom_receiver,
            write_stream,
        )
    });

    commands.spawn(Client {
        id: None,
        sender,
        thread_sender,
        receiver,
        intercom_sender,
        thread_intercom_sender,
        intercom_receiver,
        disconnected: false,
        // send: VecDeque::new(),
        // received: VecDeque::new(),
    });

    info!("Finished client setup.");
}

pub fn client_update<S, C>(
    mut client: ResMut<Client<S, C>>,
    mut received_mw: MessageWriter<ReceiveMessage<S>>,
    mut send_mr: MessageReader<SendMessage<C>>,
) where
    S: Send + Clone + Sync + 'static,
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

    while let Ok(message) = client.intercom_receiver.try_recv() {
        match message {
            IntercomResponse::ServerDisconnect => {
                warn!("Server disconnected.");
                client.disconnected = true
            }
            IntercomResponse::UnexpectedError => {
                error!("Unexpected error, this should be looked into.");
                client.disconnected = true;
            }
        }
    }
}

fn client_listen<S>(
    tx: Sender<ReceiveMessage<S>>,
    itx: Sender<IntercomResponse>,
    mut irx: Receiver<IntercomMessage>,
    mut stream: TcpStream,
) where
    S: Decode<()> + Send + Sync + 'static,
{
    loop {
        let mut len_buffer = [0u8; 4];
        if let Err(err) = stream.read_exact(&mut len_buffer) {
            error!("Failed to read length of next message in TCP stream. Error: {err}");
            itx.send(IntercomResponse::ServerDisconnect).unwrap();
            return;
        }
        let len = u32::from_be_bytes(len_buffer) as usize;

        let mut body = vec![0u8; len];
        if let Err(err) = stream.read_exact(&mut body) {
            error!(
                "Failed to read next message in TCP stream (although reading the length was successful, it was {len}). Error: {err}"
            );
            itx.send(IntercomResponse::ServerDisconnect).unwrap();
            return;
        }
        let message: S =
            match bincode_next::decode_from_slice(&body, bincode_next::config::standard()) {
                Result::Ok((message, _)) => message,
                Result::Err(err) => {
                    error!("Failed to read from TCP stream (non-fatal). Error: {err:?}");
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
            itx.send(IntercomResponse::ServerDisconnect).unwrap();
            return;
        }
    }
}

fn client_send<C>(
    mut rx: Receiver<SendMessage<C>>,
    itx: Sender<IntercomResponse>,
    mut irx: Receiver<IntercomMessage>,
    mut stream: TcpStream,
) where
    C: Encode + Clone + Send + Sync + 'static,
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
                warn!("Server disconnect detected (send thread).");
                return;
            }
            if let Err(err) = stream.write_all(&bytes) {
                error!("Failed to write message to TCP stream. Error: {err}");
                warn!("Server disconnect detected (send thread).");
                return;
            }
            if let Err(err) = stream.flush() {
                error!("Failed to flush TCP stream. Error: {err}");
                itx.send(IntercomResponse::UnexpectedError).unwrap();
                return;
            }
        }
    }
}
