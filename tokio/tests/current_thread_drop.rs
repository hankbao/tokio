//! Regression test: dropping a `current_thread::Runtime` while spawned futures
//! still own `TcpStream`s must actually close their sockets on Windows.
//!
//! On Windows, mio keeps a 0-byte overlapped read pending on every idle
//! stream. Dropping the stream issues `CancelIoEx`, but the socket is only
//! closed once the cancellation's completion packet has been reaped from the
//! IOCP port. If the runtime closes the port before that happens, the socket
//! handle leaks for the life of the process.
#![cfg(windows)]

extern crate futures;
extern crate tokio;

use futures::future;
use futures::sync::oneshot;
use futures::Future;

use std::net;
use std::os::raw::c_void;
use std::sync::mpsc;
use std::thread;

use tokio::io;
use tokio::net::TcpStream;
use tokio::runtime::current_thread::Runtime;

#[link(name = "kernel32")]
extern "system" {
    fn GetCurrentProcess() -> *mut c_void;
    fn GetProcessHandleCount(process: *mut c_void, count: *mut u32) -> i32;
}

fn handle_count() -> u32 {
    let mut count = 0u32;
    let ok = unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) };
    assert!(ok != 0, "GetProcessHandleCount failed");
    count
}

/// Number of connections parked on a pending read when the runtime is dropped.
const CONNECTIONS: usize = 64;

#[test]
fn dropping_runtime_closes_pending_sockets() {
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // The server side holds every accepted connection open until told to
    // release them, so the handle count only reflects the client side.
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let server = thread::spawn(move || {
        let conns: Vec<net::TcpStream> = listener
            .incoming()
            .take(CONNECTIONS)
            .map(|conn| conn.unwrap())
            .collect();
        let _ = release_rx.recv();
        drop(conns);
    });

    let before = handle_count();

    let mut rt = Runtime::new().unwrap();
    let mut connected = Vec::with_capacity(CONNECTIONS);

    for _ in 0..CONNECTIONS {
        let (tx, rx) = oneshot::channel::<()>();
        connected.push(rx);

        rt.spawn(
            TcpStream::connect(&addr)
                .and_then(move |stream| {
                    let _ = tx.send(());
                    // Parks forever: the server never writes anything.
                    io::read(stream, vec![0u8; 16]).map(|_| ())
                })
                .map_err(|e| panic!("connection failed: {}", e)),
        );
    }

    // Every connection is established and parked on its read.
    rt.block_on(future::join_all(connected)).unwrap();

    let during = handle_count();
    assert!(
        during >= before + CONNECTIONS as u32,
        "expected at least {} new handles while connected; before={} during={}",
        CONNECTIONS,
        before,
        during
    );

    drop(rt);

    release_tx.send(()).unwrap();
    server.join().unwrap();

    let after = handle_count();
    let margin = (CONNECTIONS / 4) as u32;
    println!(
        "process handles: before={} during={} after={}",
        before, during, after
    );
    assert!(
        after <= before + margin,
        "dropping the runtime leaked socket handles: before={} during={} after={} (margin {})",
        before,
        during,
        after,
        margin
    );
}
