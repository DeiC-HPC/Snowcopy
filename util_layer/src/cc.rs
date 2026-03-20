pub struct CcLoadAndTransfer { //TODO Jon: The Java way would be to encapsulate this data. Should that be done here with impl and traits?
    pub amount : u64,
    pub file_sem_work_time_secs: f64,
    pub file_sem_acquire_time_secs: f64,
    pub req_sem_work_time_secs: f64,
    pub req_sem_acquire_time_secs: f64,
}