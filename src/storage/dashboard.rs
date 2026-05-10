use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::protocol::Message;

pub struct DashboardServer {
    tx: broadcast::Sender<String>,
}

impl DashboardServer {
    pub fn new(tx: broadcast::Sender<String>) -> Self {
        Self { tx }
    }

    pub async fn start(self: Arc<Self>, addr: String) -> std::io::Result<()> {
        let listener = TcpListener::bind(&addr).await?;
        println!("Dashboard WebSocket server listening on: {}", addr);

        while let Ok((stream, addr)) = listener.accept().await {
            let server = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = server.handle_connection(stream, addr).await {
                    eprintln!("Error handling websocket connection from {}: {}", addr, e);
                }
            });
        }

        Ok(())
    }

    async fn handle_connection(&self, stream: TcpStream, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ws_stream = accept_async(stream).await?;
        println!("New Dashboard connection: {}", addr);

        let (mut ws_sender, _) = ws_stream.split();
        let mut rx = self.tx.subscribe();

        while let Ok(msg) = rx.recv().await {
            if ws_sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }

        println!("Dashboard connection closed: {}", addr);
        Ok(())
    }
}
