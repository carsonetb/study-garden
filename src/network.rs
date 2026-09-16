use bevy::prelude::*;

use bincode_next::{Decode, Encode};

#[derive(
    Encode, Decode, Deref, Debug, Default, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct ClientID(pub u64);

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
