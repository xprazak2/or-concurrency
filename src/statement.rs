use derive_more::Display;
use thiserror::Error;
use tokio::time;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Server {0:?}: abruptly disconnected")]
    Disconnected(ServerName),
}

#[derive(Display, Debug, PartialEq)]
#[display(fmt = "Binary[source='{}']", "from.0")]
pub struct Binary {
    #[allow(dead_code)]
    from: ServerName,
}

impl Binary {
    #[allow(dead_code)]
    pub fn new(server_name: ServerName) -> Self {
        Self { from: server_name }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerName(pub String);

pub async fn download(server_name: ServerName) -> Result<Binary, ServerError> {
    let mut interval = time::interval(time::Duration::from_millis(100));
    for _i in 0..5 {
        interval.tick().await;
        if rand::random::<f32>() < 0.1 {
            return Err(ServerError::Disconnected(server_name));
        }
    }
    Ok(Binary { from: server_name })
}
