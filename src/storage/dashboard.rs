use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::Error;

pub struct DashboardServer {
    tx: broadcast::Sender<String>,
    admin_token: String,
}

impl DashboardServer {
    pub fn new(tx: broadcast::Sender<String>, admin_token: String) -> Self {
        Self { tx, admin_token }
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

    async fn handle_connection(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let admin_token = self.admin_token.clone();

        let callback = move |req: &Request, mut res: Response| {
            let protocols = req.headers().get("Sec-WebSocket-Protocol");
            // Expecting: "bearer, <token>"
            if let Some(p) = protocols {
                let p_str = p.to_str().unwrap_or("");
                if p_str.contains(&admin_token) {
                    // Echo back the protocol to satisfy the browser
                    res.headers_mut()
                        .insert("Sec-WebSocket-Protocol", p.clone());
                    return Ok(res);
                }
            }
            // If token mismatch or missing, reject with 401
            Err(Error::Http(
                Response::builder().status(401).body(None).unwrap(),
            ))
        };

        let ws_stream = accept_hdr_async(stream, callback).await?;
        println!("New Dashboard connection: {} (Authorized)", addr);

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
