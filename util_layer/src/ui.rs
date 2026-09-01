pub enum UiEvent {
    TransferredData(u64),
    FileTransferred(),
    FileAlreadyAtTarget(u64),
    FileDiscovered(u64),
    DiscoveryFinished,
    Message(String),
    Println(String),
}