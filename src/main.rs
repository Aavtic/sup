use std::sync::{Arc, Mutex};

use std::io::Write;
use std::io::Read;
use std::io::ErrorKind;

use std::net::{TcpListener, TcpStream};
use std::net::{SocketAddrV4, Ipv4Addr};

use std::sync::mpsc::{channel, Receiver, Sender};

#[derive(Debug)]
struct MessagingService {
    connections: Arc<Mutex<Vec<TcpStream>>>,
    sender: Sender<String>,
    receiver: Option<std::sync::mpsc::Receiver<String>>,
}

impl MessagingService {
    fn new() -> Self {
        let (sender, receiver) = channel();
        Self {
            connections: Arc::new(Mutex::new(Vec::new())),
            sender,
            receiver: Some(receiver),
        }
    }

    fn get_sender(&self) -> Sender<String> {
        return self.sender.clone();
    }

    fn add_connection(&mut self, conn: TcpStream) {
        self.connections.lock().unwrap().push(conn);
    }

    fn start_messaging_service_background(&mut self) {
        let receiver = self.receiver.take().unwrap();
        let connections = Arc::clone(&self.connections);

        std::thread::spawn(move || {
            while let Ok(msg) = receiver.recv() {

                let mut connections = connections.lock().unwrap();

                connections.retain_mut(|conn| {
                    match conn.write_all(msg.as_bytes()) {
                        Ok(_) => true,
                        Err(err) => {
                            match err.kind() {
                                ErrorKind::BrokenPipe => false,
                                _ => {
                                    eprintln!("ERROR: Could not send message: {} to connection due to : {}. Closing Connection", msg, err);
                                    _ = conn.shutdown(std::net::Shutdown::Both);
                                    false
                                }
                            }
                        }
                    }
                })
            }
        });
    }
}

fn get_user_input() -> String {
    let mut input = String::new();

    print!("Enter broadcast message: ");

    std::io::stdout().lock().flush().unwrap();
    std::io::stdin().read_line(&mut input).unwrap();

    return input
}

fn handle(mut conn: TcpStream, conn_addr: std::net::SocketAddr, broadcast_sender: Sender<String>) {
    println!("Connection accepted from: {}", conn_addr.ip().to_string());

    let mut data: [u8; 1024] = [0; 1024];

    loop {

        match conn.read(&mut data) {
            Ok(0) => {
                println!("Client {} disconnected", conn.peer_addr().unwrap());
                return;
            }
            Ok(_) => {
                if let Ok(data) = std::str::from_utf8(&data) {
                    println!("{}", data);
                } else {
                    println!("Could not read incoming data to utf8");
                    continue;
                }

                //
                // broadcast message to other connections
                //

                broadcast_sender.send(std::str::from_utf8(&data).unwrap().to_string()).unwrap();

                data.fill(0);
            },

            Err(err) => match err.kind() {
                    ErrorKind::ConnectionReset |
                    ErrorKind::ConnectionAborted |
                    ErrorKind::BrokenPipe  => {
                        println!("Client {} disconnected", conn.peer_addr().unwrap());
                        return;
                }

                    _ => {
                        eprintln!("Could not receive from client due to: {}\nClosing Connection", err);
                        conn.shutdown(std::net::Shutdown::Both).unwrap();
                }
            }
        }
    }
}

fn listen_and_serve_connections(server: TcpListener, messaging_service: Arc<Mutex<MessagingService>>) {
    std::thread::spawn(move || {
        loop {
            if let Ok((conn, conn_addr)) = server.accept() {
                let mut messaging_service_lock = messaging_service.lock().unwrap();
                messaging_service_lock.add_connection(conn.try_clone().unwrap());
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
    sender:             Sender<String>,
) {

    std::thread::spawn( move || {
        loop {
            if let Ok(data) = user_input_receiver.lock().unwrap().recv() {
                sender.send(data).unwrap();
            }
        }
    }
    );
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

    broadcast_message_to_connections(Arc::new(Mutex::new(user_input_receiver)), messaging_service_sender);

    loop {
        let user_input = get_user_input();
        user_input_sender.send(user_input).unwrap();
    }
}

fn main() {
    server();
}

