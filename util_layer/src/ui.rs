pub enum UiEvent {
    TransferredData(u64),
    FileTransferred(u64),
    FileAlreadyAtTarget(u64),
    FileDiscovered(u64),
    DiscoveryFinished,
    Message(String),
    Println(String),
}