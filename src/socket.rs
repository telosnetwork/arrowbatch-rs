use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use futures_util::{StreamExt, SinkExt};
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
struct RPCResult {
    id: String,
    result: Option<serde_json::Value>,
    error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RPCRequest {
    id: String,
    method: String,
    params: serde_json::Value,
}

type RPCResultSender = oneshot::Sender<Result<RPCResult, String>>;
type RPCResultReceiver = oneshot::Receiver<Result<RPCResult, String>>;

struct RPCSession {
    rpc_results: Arc<Mutex<HashMap<String, RPCResultSender>>>,
    request_tx: mpsc::UnboundedSender<(String, serde_json::Value, RPCResultSender)>,
}

impl RPCSession {
    // Initialize a new RPC session
    async fn new(url: &str) -> Self {
        // Connect to the WebSocket server
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
        let (mut write, mut read) = ws_stream.split();
        let rpc_results = Arc::new(Mutex::new(HashMap::new()));
        let (request_tx, mut request_rx) = mpsc::unbounded_channel();

        let rpc_results_clone: Arc<Mutex<HashMap<String, RPCResultSender>>> = Arc::clone(&rpc_results);

        // Spawn a task to handle incoming messages from the WebSocket
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                if let Ok(msg) = msg {
                    // Parse the incoming message as an RPCResult
                    if let Ok(result) = serde_json::from_str::<RPCResult>(&msg.to_string()) {
                        // Send the result to the corresponding sender if exists
                        if let Some(sender) = rpc_results_clone.lock().await.remove(&result.id) {
                            let _ = sender.send(Ok(result));
                        } else {
                            eprintln!("Unexpected ws msg: {}", msg);
                        }
                    } else {
                        eprintln!("Unhandled RPC msg!?\n{}", msg);
                    }
                }
            }
        });

        let rpc_results_clone: Arc<Mutex<HashMap<String, RPCResultSender>>> = Arc::clone(&rpc_results);

        // Spawn a task to handle outgoing requests
        tokio::spawn(async move {
            while let Some((method, params, sender)) = request_rx.recv().await {
                // Generate a new UUID for each request
                let id = Uuid::new_v4().to_string();

                let msg = RPCRequest {
                    id: id.clone(),
                    method,
                    params,
                };

                // Insert the sender into the hashmap with the UUID as the key
                rpc_results_clone.lock().await.insert(id, sender);
                let msg = Message::Text(serde_json::to_string(&msg).unwrap());
                // Send the request message through the WebSocket
                write.send(msg).await.unwrap();
            }
        });

        Self {
            rpc_results,
            request_tx,
        }
    }

    // Send an RPC request
    async fn send_request(&self, method: String, params: serde_json::Value) -> RPCResultReceiver {
        let (sender, receiver) = oneshot::channel();
        let _ = self.request_tx.send((method, params, sender));
        receiver
    }
}

// Test for the RPCSession
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_rpc_session() {
        // Initialize the RPC session
        let rpc_session = RPCSession::new("ws://localhost:4201").await;

        // Send get_info request
        let response = rpc_session.send_request("get_info".to_string(), serde_json::json!({})).await;
        let result = response.await.unwrap().unwrap();
        println!("get_info Response: {:?}", result);

        // Extract lastOrdinal from the get_info result
        let mut res_str: String = result.result.unwrap().get("lastOrdinal").unwrap().to_string();
        res_str.retain(|c| c != '\"');
        println!("lo: {}", res_str);
        let last_ordinal: u64 = res_str.parse().unwrap();

        // Send get_row request with the lastOrdinal obtained from get_info
        let response = rpc_session.send_request("get_row".to_string(), serde_json::json!({"ordinal": last_ordinal})).await;
        let result = response.await.unwrap().unwrap();
        println!("get_row Response: {:#?}", result);

        // Sleep for a short duration to ensure all async tasks are complete
        sleep(Duration::from_millis(100)).await;
    }
}

