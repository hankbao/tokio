//! An example of hooking up stdin/stdout to either a TCP or UDP stream.
//!
//! This example will connect to a socket address specified in the argument list
//! and then forward all data read on stdin to the server, printing out all data
//! received on stdout. An optional `--udp` argument can be passed to specify
//! that the connection should be made over UDP instead of TCP, translating each
//! line entered on stdin to a UDP packet to be sent to the remote address.
//!
//! Note that this is not currently optimized for performance, especially
//! around buffer management. Rather it's intended to show an example of
//! working with a client.
//!
//! This example can be quite useful when interacting with the other examples in
//! this repository! Many of them recommend running this as a simple "hook up
//! stdin/stdout to a server" to get up and running.

extern crate bytes;
extern crate futures;
extern crate tokio;
extern crate tokio_io;

use std::env;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::thread;

use futures::sync::mpsc;
use tokio::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Determine if we're going to run in TCP or UDP mode
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    let tcp = match args.iter().position(|a| a == "--udp") {
        Some(i) => {
            args.remove(i);
            false
        }
        None => true,
    };

    // Parse what address we're going to connect to
    let addr = match args.first() {
        Some(addr) => addr,
        None => Err("this program requires at least one argument")?,
    };
    let addr = addr.parse::<SocketAddr>()?;

    // Right now Tokio doesn't support a handle to stdin running on the event
    // loop, so we farm out that work to a separate thread. This thread will
    // read data (with blocking I/O) from stdin and then send it to the event
    // loop over a standard futures channel.
    let (stdin_tx, stdin_rx) = mpsc::channel(0);
    thread::spawn(|| read_stdin(stdin_tx));
    let stdin_rx = stdin_rx.map_err(|_| panic!("errors not possible on rx"));

    // Now that we've got our stdin read we either set up our TCP connection or
    // our UDP connection to get a stream of bytes we're going to emit to
    // stdout.
    let stdout = if tcp {
        tcp::connect(&addr, Box::new(stdin_rx))?
    } else {
        udp::connect(&addr, Box::new(stdin_rx))?
    };

    // And now with our stream of bytes to write to stdout, we execute that in
    // the event loop! Note that this is doing blocking I/O to emit data to
    // stdout, and in general it's a no-no to do that sort of work on the event
    // loop. In this case, though, we know it's ok as the event loop isn't
    // otherwise running anything useful.
    let mut out = io::stdout();

    tokio::run({
        stdout
            .for_each(move |chunk| out.write_all(&chunk))
            .map_err(|e| println!("error reading stdout; error = {:?}", e))
    });
    Ok(())
}

mod codec {
    use bytes::{BufMut, BytesMut};
    use std::io;
    use tokio::codec::{Decoder, Encoder};

    /// A simple `Codec` implementation that just ships bytes around.
    ///
    /// This type is used for "framing" a TCP/UDP stream of bytes but it's really
    /// just a convenient method for us to work with streams/sinks for now.
    /// This'll just take any data read and interpret it as a "frame" and
    /// conversely just shove data into the output location without looking at
    /// it.
    pub struct Bytes;

    impl Decoder for Bytes {
        type Item = BytesMut;
        type Error = io::Error;

        fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<BytesMut>> {
            if buf.len() > 0 {
                let len = buf.len();
                Ok(Some(buf.split_to(len)))
            } else {
                Ok(None)
            }
        }
    }

    impl Encoder for Bytes {
        type Item = Vec<u8>;
        type Error = io::Error;

        fn encode(&mut self, data: Vec<u8>, buf: &mut BytesMut) -> io::Result<()> {
            buf.put(&data[..]);
            Ok(())
        }
    }
}

mod tcp {
    use tokio;
    use tokio::codec::Decoder;
    use tokio::net::TcpStream;
    use tokio::prelude::*;

    use bytes::BytesMut;
    use codec::Bytes;

    use std::error::Error;
    use std::io;
    use std::net::SocketAddr;

    pub fn connect(
        addr: &SocketAddr,
        stdin: Box<dyn Stream<Item = Vec<u8>, Error = io::Error> + Send>,
    ) -> Result<Box<dyn Stream<Item = BytesMut, Error = io::Error> + Send>, Box<dyn Error>> {
        let tcp = TcpStream::connect(addr);

        // After the TCP connection has been established, we set up our client
        // to start forwarding data.
        //
        // First we use the `Io::framed` method with a simple implementation of
        // a `Codec` (listed below) that just ships bytes around. We then split
        // that in two to work with the stream and sink separately.
        //
        // Half of the work we're going to do is to take all data we receive on
        // `stdin` and send that along the TCP stream (`sink`). The second half
        // is to take all the data we receive (`stream`) and then write that to
        // stdout. We'll be passing this handle back out from this method.
        //
        // You'll also note that we *spawn* the work to read stdin and write it
        // to the TCP stream. This is done to ensure that happens concurrently
        // with us reading data from the stream.
        let stream = Box::new(
            tcp.map(move |stream| {
                let (sink, stream) = Bytes.framed(stream).split();

                tokio::spawn(stdin.forward(sink).then(|result| {
                    if let Err(e) = result {
                        println!("failed to write to socket: {}", e)
                    }
                    Ok(())
                }));

                stream
            })
            .flatten_stream(),
        );
        Ok(stream)
    }
}

mod udp {
    use std::error::Error;
    use std::io;
    use std::net::SocketAddr;

    use bytes::BytesMut;
    use tokio;
    use tokio::net::{UdpFramed, UdpSocket};
    use tokio::prelude::*;

    use codec::Bytes;

    pub fn connect(
        &addr: &SocketAddr,
        stdin: Box<dyn Stream<Item = Vec<u8>, Error = io::Error> + Send>,
    ) -> Result<Box<dyn Stream<Item = BytesMut, Error = io::Error> + Send>, Box<dyn Error>> {
        // We'll bind our UDP socket to a local IP/port, but for now we
        // basically let the OS pick both of those.
        let addr_to_bind = if addr.ip().is_ipv4() {
            "0.0.0.0:0".parse()?
        } else {
            "[::]:0".parse()?
        };
        let udp = match UdpSocket::bind(&addr_to_bind) {
            Ok(udp) => udp,
            Err(_) => Err("failed to bind socket")?,
        };

        // Like above with TCP we use an instance of `Bytes` codec to transform
        // this UDP socket into a framed sink/stream which operates over
        // discrete values. In this case we're working with *pairs* of socket
        // addresses and byte buffers.
        let (sink, stream) = UdpFramed::new(udp, Bytes).split();

        // All bytes from `stdin` will go to the `addr` specified in our
        // argument list. Like with TCP this is spawned concurrently
        let forward_stdin = stdin
            .map(move |chunk| (chunk, addr))
            .forward(sink)
            .then(|result| {
                if let Err(e) = result {
                    println!("failed to write to socket: {}", e)
                }
                Ok(())
            });

        // With UDP we could receive data from any source, so filter out
        // anything coming from a different address
        let receive = stream.filter_map(move |(chunk, src)| {
            if src == addr {
                Some(chunk.into())
            } else {
                None
            }
        });

        let stream = Box::new(
            future::lazy(|| {
                tokio::spawn(forward_stdin);
                future::ok(receive)
            })
            .flatten_stream(),
        );
        Ok(stream)
    }
}

// Our helper method which will read data from stdin and send it along the
// sender provided.
fn read_stdin(mut tx: mpsc::Sender<Vec<u8>>) {
    let mut stdin = io::stdin();
    loop {
        let mut buf = vec![0; 1024];
        let n = match stdin.read(&mut buf) {
            Err(_) | Ok(0) => break,
            Ok(n) => n,
        };
        buf.truncate(n);
        tx = match tx.send(buf).wait() {
            Ok(tx) => tx,
            Err(_) => break,
        };
    }
}
