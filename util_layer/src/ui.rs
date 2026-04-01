pub enum UiEvent {
    AddToTotal(u64),
    Uploaded(u64),
    FileTransferred(u64),
    FileAlreadyAtTarget(u64),
    TotalDiscovered(u64, u64),
    DiscoveryFinished,
    Message(String),
    Println(String),
}