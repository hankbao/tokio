//! Regression test: dropping a `current_thread::Runtime` while spawned
//! futures are parked on a pending **write** must also close their sockets on
//! Windows.
//!
//! A write that the peer never drains keeps an overlapped `WSASend` in
//! flight, which holds a forgotten reference to the mio stream. Since
//! hankbao/mio `v0.6.x-windows` cancels pending writes on drop, the runtime's
//! drop-time reactor pump can reap the cancellation and the socket gets
//! closed; with the older mio these sockets leaked for the life of the
//! process.
//!
//! The client sockets use `SO_SNDBUF = 0`, so an overlapped send only
//! completes once the peer consumes the data -- and the server accepts but
//! never reads. The first oversized write is swallowed whole by mio's
//! internal buffer (reported complete right away) and leaves that `WSASend`
//! in flight; a second, chained one-byte write then returns `WouldBlock`
//! until the send finishes -- which it never does -- so the future still owns
//! the stream when the runtime is dropped.
#![cfg(windows)]

extern crate futures;
extern crate tokio;

use futures::future;
use futures::sync::oneshot;
use futures::Future;

use std::net;
use std::os::raw::{c_char, c_int, c_void};
use std::os::windows::io::AsRawSocket;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use tokio::io;
use tokio::net::TcpStream;
use tokio::runtime::current_thread::Runtime;
use tokio::timer::Delay;

#[link(name = "kernel32")]
extern "system" {
    fn GetCurrentProcess() -> *mut c_void;
    fn GetProcessHandleCount(process: *mut c_void, count: *mut u32) -> i32;
}

#[link(name = "ws2_32")]
extern "system" {
    fn setsockopt(
        s: usize,
        level: c_int,
        optname: c_int,
        optval: *const c_char,
        optlen: c_int,
    ) -> c_int;
}

const SOL_SOCKET: c_int = 0xFFFF;
const SO_SNDBUF: c_int = 0x1001;

/// With a zero send buffer the kernel does not copy outgoing data, so an
/// overlapped send only completes once the peer has consumed it.
fn set_zero_sndbuf(socket: &dyn AsRawSocket) {
    let zero: c_int = 0;
    let ret = unsafe {
        setsockopt(
            socket.as_raw_socket() as usize,
            SOL_SOCKET,
            SO_SNDBUF,
            &zero as *const c_int as *const c_char,
            ::std::mem::size_of::<c_int>() as c_int,
        )
    };
    assert_eq!(ret, 0, "setsockopt(SO_SNDBUF, 0) failed");
}

fn handle_count() -> u32 {
    let mut count = 0u32;
    let ok = unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) };
    assert!(ok != 0, "GetProcessHandleCount failed");
    count
}

/// Number of connections parked on a pending write when the runtime is
/// dropped.
const CONNECTIONS: usize = 16;

/// Well beyond what the peer side absorbs with a non-reading server
/// (measured ~2.3 MiB), so the `WSASend` is still in flight when the runtime
/// is dropped.
const WRITE_SIZE: usize = 6 * 1024 * 1024;

#[test]
fn dropping_runtime_closes_write_pending_sockets() {
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // The server side accepts but never reads, so the client writes stall
    // once the kernel buffers are full; the connections are held open until
    // released so the handle count only reflects the client side.
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
    let completed = Arc::new(AtomicUsize::new(0));

    for _ in 0..CONNECTIONS {
        let (tx, rx) = oneshot::channel::<()>();
        connected.push(rx);
        let completed = completed.clone();

        rt.spawn(
            TcpStream::connect(&addr)
                .and_then(move |stream| {
                    let _ = tx.send(());
                    set_zero_sndbuf(&stream);
                    // The first write is swallowed whole by mio's internal
                    // buffer and leaves an overlapped send in flight that the
                    // non-reading server never lets finish; the chained
                    // second write blocks on it, so the future keeps the
                    // stream alive until the runtime is dropped.
                    io::write_all(stream, vec![0u8; WRITE_SIZE])
                        .and_then(|(stream, _)| io::write_all(stream, [0u8; 1]))
                        .map(move |_| {
                            completed.fetch_add(1, Ordering::SeqCst);
                        })
                })
                .map_err(|e| panic!("connection failed: {}", e)),
        );
    }

    // Every connection is established...
    rt.block_on(future::join_all(connected)).unwrap();

    // ... and given time to fill the kernel buffers and park with an
    // overlapped write still in flight.
    rt.block_on(Delay::new(Instant::now() + Duration::from_millis(500)))
        .unwrap();

    let during = handle_count();
    assert!(
        during >= before + CONNECTIONS as u32,
        "expected at least {} new handles while connected; before={} during={}",
        CONNECTIONS,
        before,
        during
    );
    assert_eq!(
        completed.load(Ordering::SeqCst),
        0,
        "no write should have completed; the test is not exercising \
         write-pending teardown"
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
        "dropping the runtime leaked write-pending socket handles: \
         before={} during={} after={} (margin {})",
        before,
        during,
        after,
        margin
    );
}
