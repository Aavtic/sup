use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use std::io::{BufRead, BufReader};
use std::io::ErrorKind;
use std::io::Write;

use std::net::{Ipv4Addr, SocketAddrV4, SocketAddr};
use std::net::{TcpListener, TcpStream};

use std::sync::mpsc::{Receiver, Sender, channel};

use serde::{Serialize, Deserialize};


#[derive(Debug, Serialize, Deserialize)]
struct ServerBroadcast {
    from: String,
    message: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct NewMessage {
    from: String,
    message: String,
    connection: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
struct NewMessageIn {
    message: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct NewMessageOut {
    from: String,
    message: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserInternal {
    name: String,
    connection: SocketAddr,
}


#[derive(Debug, Serialize, Deserialize)]
enum Update {
    NewMessage(NewMessage),
    ServerBroadcast(ServerBroadcast),
    Disconnected(SocketAddr),
    NewUser(UserInternal),
}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    name: String,
}
#[derive(Debug, Serialize, Deserialize)]
enum ClientUpdate {
    NewMessage(NewMessageIn),
    NewUser(User),
    Disconnected(SocketAddr),
}

#[derive(Debug)]
struct MessagingService {
    connections: Arc<Mutex<Vec<TcpStream>>>,
    users: Arc<Mutex<HashMap<SocketAddr, User>>>,
    sender: Sender<Update>,
    receiver: Option<std::sync::mpsc::Receiver<Update>>,
}

impl MessagingService {
    fn new() -> Self {
        let (sender, receiver) = channel();
        Self {
            connections: Arc::new(Mutex::new(Vec::new())),
            users: Arc::new(Mutex::new(HashMap::new())),
            sender,
            receiver: Some(receiver),
        }
    }

    fn get_sender(&self) -> Sender<Update> {
        return self.sender.clone();
    }

    fn add_connection(&mut self, conn: TcpStream) {
        self.connections.lock().unwrap().push(conn);
    }

    fn try_send_to_all_connections(conn: &mut TcpStream, user_name: String, message: String) -> bool {
        let new_message_out = NewMessageOut { from: user_name.clone(), message: message.clone() };

        let new_message_json = serde_json::to_vec(&new_message_out).unwrap();

        match conn.write_all(&new_message_json) {
            Ok(_) => true,
            Err(err) => {
                match err.kind() {
                    ErrorKind::BrokenPipe => false,
                    _ => {
                        eprintln!("ERROR: Could not send message: {} to connection due to : {}. Closing Connection",message, err);
                        _ = conn.shutdown(std::net::Shutdown::Both);
                        false
                    }
                }
            }
        }
    }


    fn start_messaging_service_background(&mut self) {
        let receiver = self.receiver.take().unwrap();
        let connections = Arc::clone(&self.connections);
        let users    =    Arc::clone(&self.users);

        std::thread::spawn(move || {
            while let Ok(update) = receiver.recv() {

                match update {
                    Update::NewUser(user_internal) => {
                        println!("[{}] Joined the chat", user_internal.name.clone());

                        users.lock().unwrap().insert(user_internal.connection, User{ name: user_internal.name });
                    }

                    Update::NewMessage(new_message) => {
                        let mut connections = connections.lock().unwrap();
                        let user_name;
                        if let Some(user) = users.lock().unwrap().get(&new_message.connection) {
                            user_name = user.name.clone();
                        } else {
                            user_name = "NOTFOUND".into();
                        }

                        println!("[{}]: {}", user_name, new_message.message);

                        connections.retain_mut(|conn| Self::try_send_to_all_connections(conn, user_name.clone(), new_message.message.clone()));
                    },


                    Update::ServerBroadcast(broadcast) => {
                        let mut connections = connections.lock().unwrap();
                        let user_name = broadcast.from;

                        println!("[{}]: {}", user_name, broadcast.message);

                        connections.retain_mut(|conn| Self::try_send_to_all_connections(conn, user_name.clone(), broadcast.message.clone()));
                    }

                    Update::Disconnected(addr) => {
                        //
                        // just needs to be removed from users as elements in the connections list
                        // is automatically removed once a broadcast message to them fails
                        //

                        let Some(user) = users.lock().unwrap().remove(&addr) else { continue };
                        println!("[{}] DISCONNECTED", user.name);
                    }

                }
            }

        });
    }
}


fn get_client_update(reader: &mut BufReader<&TcpStream>, addr: &SocketAddr) -> Result<ClientUpdate, Box<dyn std::error::Error>> {
    let mut line = String::new();

    let bytes = reader.read_line(&mut line)?;

    // Client disconnected
    if bytes == 0 {
        return Ok(ClientUpdate::Disconnected(addr.clone()));
    }

    let update = serde_json::from_str(&line)?;

    Ok(update)
}

fn get_user_input() -> String {
    let mut input = String::new();

    print!("Enter broadcast message: ");

    std::io::stdout().lock().flush().unwrap();
    std::io::stdin().read_line(&mut input).unwrap();

    return input;
}


fn handle(conn: TcpStream, conn_addr: std::net::SocketAddr, messaging_service: Sender<Update>) {
    //println!("Connection accepted from: {}", conn_addr.ip().to_string());

    let mut user_created = false;
    
    let mut reader = BufReader::new(&conn);

    loop {
        let Ok(update) = get_client_update(&mut reader, &conn_addr) else {
            println!("UNSUPPORTED MESSAGE RECEIVED");
            continue
        };

        match update {
            ClientUpdate::NewUser(user) => {
                messaging_service.send(Update::NewUser(UserInternal{ name: user.name, connection: conn_addr })).unwrap();

                user_created = true;
            }

            ClientUpdate::NewMessage(new_message) => {
                if !user_created { continue }

                messaging_service.
                    send(Update::NewMessage(NewMessage{
                        from: "".into(), 
                        connection: conn_addr.clone(),
                        message: new_message.message,
                    })).unwrap();
            }

           ClientUpdate::Disconnected(addr) => {
                _ = conn.shutdown(std::net::Shutdown::Both);
                messaging_service.send(Update::Disconnected(addr)).unwrap();
                return;
            }
        }
    }
}

fn listen_and_serve_connections(
    server: TcpListener,
    messaging_service: Arc<Mutex<MessagingService>>,
) {
    std::thread::spawn(move || {
        loop {
            if let Ok((conn, conn_addr)) = server.accept() {
                let mut messaging_service_lock = messaging_service.lock().unwrap();
                let connection_clone = conn.try_clone().unwrap();
                messaging_service_lock.add_connection(connection_clone);
                let messaging_service_sender = messaging_service_lock.get_sender();

                _ = std::thread::spawn(move || {
                    handle(conn, conn_addr, messaging_service_sender);
                });
            }
        }
    });
}

fn broadcast_message_to_connections(
    user_input_receiver: Arc<Mutex<Receiver<String>>>,
    sender: Sender<Update>,
) {
    std::thread::spawn(move || {
        loop {
            if let Ok(data) = user_input_receiver.lock().unwrap().recv() {
                sender.send(Update::ServerBroadcast(
                        ServerBroadcast{
                            from: "[SERVER]".into(),
                            message: data,
                        }
                )).unwrap();
            }
        }
    });
}

fn server() {
    let mut messaging_service = MessagingService::new();
    messaging_service.start_messaging_service_background();

    let (user_input_sender, user_input_receiver) = channel::<String>();

    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8081);
    let server = TcpListener::bind(addr).unwrap();

    println!("[+] Server up and running in {}:{}", addr.ip(), addr.port());

    let messaging_service_sender = messaging_service.get_sender();
    let messaging_service_mut = Arc::new(Mutex::new(messaging_service));

    let messaging_service_connections = Arc::clone(&messaging_service_mut);
    listen_and_serve_connections(server, messaging_service_connections);

    broadcast_message_to_connections(
        Arc::new(Mutex::new(user_input_receiver)),
        messaging_service_sender,
    );

    loop {
        let user_input = get_user_input();
        user_input_sender.send(user_input).unwrap();
    }
}

fn main() {
    server();
}
