//! Regression tests for the reactor pump a dropped `current_thread::Runtime`
//! runs on Windows.
//!
//! On Windows, dropping a socket with an overlapped operation in flight only
//! *cancels* that operation; mio closes the socket once the cancellation's
//! completion packet has been reaped from the IOCP port. Nobody reaps after
//! the reactor is gone, so a runtime being dropped turns its reactor until
//! every pending operation has been accounted for. These tests cover the
//! cases the original fixed-size pump (four turns of at most 1024
//! completions each, skipped while unwinding and skipped when no spawned
//! future was pending) got wrong:
//!
//! * more cancellations than four turns can reap: the rest leaked;
//! * a runtime dropped while its thread unwinds: the pump was skipped;
//! * sockets dropped inside `block_on` by the future it ran: those never
//!   counted as pending futures, so the pump was skipped as well;
//! * and the cost of all this when nothing is pending.
//!
//! Handle counts are process-wide, so the tests run one at a time. The
//! server side lets go of its connections with a reset: by then the client
//! sockets have been closed and the reset takes their half-closed entries
//! with it, instead of parking thousands of client ports in `TIME_WAIT`
//! for the next minutes and exhausting the dynamic port range (16384 ports
//! by default) when the tests are run again soon after.
#![cfg(windows)]

extern crate futures;
extern crate tokio;

use futures::future;
use futures::sync::oneshot;
use futures::Future;

use std::mem;
use std::net;
use std::os::raw::{c_char, c_int, c_void};
use std::os::windows::io::AsRawSocket;
use std::sync::{mpsc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use tokio::io;
use tokio::net::TcpStream;
use tokio::runtime::current_thread::Runtime;

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
const SO_LINGER: c_int = 0x0080;

#[repr(C)]
struct Linger {
    l_onoff: u16,
    l_linger: u16,
}

fn handle_count() -> u32 {
    let mut count = 0u32;
    let ok = unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) };
    assert!(ok != 0, "GetProcessHandleCount failed");
    count
}

/// Makes closing the socket reset the connection instead of going through
/// the orderly shutdown (see the module documentation).
fn reset_on_close(socket: &dyn AsRawSocket) {
    let linger = Linger {
        l_onoff: 1,
        l_linger: 0,
    };
    let ret = unsafe {
        setsockopt(
            socket.as_raw_socket() as usize,
            SOL_SOCKET,
            SO_LINGER,
            &linger as *const Linger as *const c_char,
            mem::size_of::<Linger>() as c_int,
        )
    };
    assert_eq!(ret, 0, "setsockopt(SO_LINGER) failed");
}

/// Serializes the tests: they compare process-wide handle counts and time
/// the drop, neither of which survives another test running concurrently.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Number of connections parked on a pending read when the runtime is
/// dropped, for the tests that do not need more.
const CONNECTIONS: usize = 64;

/// More parked connections than the original pump (four turns of at most
/// 1024 completions each) could ever reap.
const OVER_CAP_CONNECTIONS: usize = 6144;

/// Connections established per `block_on` in the over-cap test, so the
/// listener backlog is never exceeded.
const CONNECT_BATCH: usize = 512;

/// The server side of the tests: accepts `count` connections and holds them
/// open until `release` is called, so that a leaked *client* socket is what
/// the handle count reflects once the server is gone.
struct HoldingServer {
    release: mpsc::Sender<()>,
    thread: thread::JoinHandle<()>,
}

impl HoldingServer {
    fn start(listener: net::TcpListener, count: usize) -> HoldingServer {
        let (release, released) = mpsc::channel::<()>();
        let thread = thread::spawn(move || {
            let conns: Vec<net::TcpStream> = listener
                .incoming()
                .take(count)
                .map(|conn| {
                    let conn = conn.unwrap();
                    reset_on_close(&conn);
                    conn
                })
                .collect();
            let _ = released.recv();
            drop(conns);
        });
        HoldingServer { release, thread }
    }

    /// Drops every held connection and waits for the server to be gone.
    fn release(self) {
        self.release.send(()).unwrap();
        self.thread.join().unwrap();
    }
}

/// Makes one connection through a throwaway runtime and drops it, so the
/// handles that the first connection of a process allocates for good
/// (Winsock, the `AcceptEx` / `ConnectEx` extension lookups: about seven of
/// them) are already there when the baseline handle count is taken. The
/// server must be prepared to accept this connection too.
fn warm_up(addr: &net::SocketAddr) {
    let mut rt = Runtime::new().unwrap();
    rt.block_on(TcpStream::connect(addr).map(|_| ())).unwrap();
    drop(rt);
}

/// Spawns a connection that parks forever on a read (the server never writes
/// anything) and returns a receiver that fires once it is established.
fn spawn_parked_read(rt: &mut Runtime, addr: &net::SocketAddr) -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel::<()>();
    rt.spawn(
        TcpStream::connect(addr)
            .and_then(move |stream| {
                let _ = tx.send(());
                io::read(stream, vec![0u8; 16]).map(|_| ())
            })
            .map_err(|e| panic!("connection failed: {}", e)),
    );
    rx
}

fn assert_connected(before: u32, during: u32, connections: usize) {
    assert!(
        during >= before + connections as u32,
        "expected at least {} new handles while connected; before={} during={}",
        connections,
        before,
        during
    );
}

fn assert_no_leak(what: &str, before: u32, during: u32, after: u32, margin: u32) {
    println!(
        "process handles: before={} during={} after={}",
        before, during, after
    );
    assert!(
        after <= before + margin,
        "dropping the runtime leaked {}: before={} during={} after={} (margin {})",
        what,
        before,
        during,
        after,
        margin
    );
}

/// Dropping a runtime with more cancellations pending than the old pump
/// could reap must still close every socket, and must not take long: the
/// reaping is bounded by progress, not by a fixed number of turns.
#[test]
fn dropping_runtime_reaps_beyond_the_old_cap() {
    let _serial = serial();
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = HoldingServer::start(listener, OVER_CAP_CONNECTIONS + 1);

    warm_up(&addr);
    let before = handle_count();

    let mut rt = Runtime::new().unwrap();
    for _ in 0..OVER_CAP_CONNECTIONS / CONNECT_BATCH {
        let batch: Vec<_> = (0..CONNECT_BATCH)
            .map(|_| spawn_parked_read(&mut rt, &addr))
            .collect();
        rt.block_on(future::join_all(batch)).unwrap();
    }

    let during = handle_count();
    assert_connected(before, during, OVER_CAP_CONNECTIONS);

    let start = Instant::now();
    drop(rt);
    let took = start.elapsed();

    server.release();

    let after = handle_count();
    // Far below the 2048 sockets the old four-turn pump could never reap
    // (the count settles a few handles *below* the baseline: the server
    // side of the warm-up connection is gone too).
    let margin = (OVER_CAP_CONNECTIONS / 64) as u32;
    println!("dropping the runtime took {:?}", took);
    assert_no_leak("socket handles beyond the old cap", before, during, after, margin);
    // 2048 parked TCP streams reap in ~48 ms; the pump must not be padding
    // that with idle turns.
    assert!(
        took < Duration::from_secs(1),
        "dropping the runtime took {:?}",
        took
    );
}

/// A runtime dropped while its thread unwinds must close its sockets too:
/// a panicking task is the one exit path that must not leak.
#[test]
fn dropping_runtime_while_unwinding_closes_pending_sockets() {
    let _serial = serial();
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = HoldingServer::start(listener, CONNECTIONS + 1);

    warm_up(&addr);
    let before = handle_count();

    let (during_tx, during_rx) = mpsc::channel::<u32>();
    let worker = thread::spawn(move || {
        let mut rt = Runtime::new().unwrap();
        let connected: Vec<_> = (0..CONNECTIONS)
            .map(|_| spawn_parked_read(&mut rt, &addr))
            .collect();
        rt.block_on(future::join_all(connected)).unwrap();
        during_tx.send(handle_count()).unwrap();

        // The executor does not catch panics from the futures it runs: the
        // panicking task takes `block_on` down with it, and `rt`, a local of
        // this closure, is dropped while the thread unwinds.
        rt.spawn(future::lazy(|| -> Result<(), ()> {
            panic!("task panicked on purpose")
        }));
        let _ = rt.block_on(future::empty::<(), ()>());
        unreachable!("block_on returned instead of unwinding");
    });

    assert!(worker.join().is_err(), "the worker thread should have panicked");
    let during = during_rx.recv().unwrap();
    assert_connected(before, during, CONNECTIONS);

    server.release();

    let after = handle_count();
    let margin = (CONNECTIONS / 4) as u32;
    assert_no_leak("socket handles while unwinding", before, during, after, margin);
}

/// Sockets dropped by the future `block_on` runs are cancelled without the
/// reactor being turned again, and the runtime is idle afterwards; dropping
/// it must still reap those cancellations.
#[test]
fn dropping_idle_runtime_closes_sockets_dropped_inside_block_on() {
    let _serial = serial();
    let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = HoldingServer::start(listener, CONNECTIONS + 1);

    warm_up(&addr);
    let before = handle_count();

    let mut rt = Runtime::new().unwrap();

    // The future keeps every stream until all of them are connected and
    // drops them together when it completes, that is inside the last poll
    // `block_on` makes: each stream has the 0-byte read mio issues on
    // connect in flight, so each drop leaves a cancellation behind, and
    // `block_on` returns without turning the reactor again. Nothing was
    // spawned, so the runtime is idle from its own point of view.
    let connects: Vec<_> = (0..CONNECTIONS)
        .map(|_| TcpStream::connect(&addr))
        .collect();
    rt.block_on(future::join_all(connects).map(|streams| {
        assert_eq!(streams.len(), CONNECTIONS);
    }))
    .unwrap();

    let during = handle_count();
    assert_connected(before, during, CONNECTIONS);

    drop(rt);

    server.release();

    let after = handle_count();
    let margin = (CONNECTIONS / 4) as u32;
    assert_no_leak("sockets dropped inside block_on", before, during, after, margin);
}

/// With nothing pending the pump still runs its floor of four turns, each of
/// which the IOCP wait rounds up to the 15.625 ms timer granularity (~63 ms
/// in total, measured); it must not cost noticeably more than that.
#[test]
fn dropping_idle_runtime_costs_only_the_floor() {
    let _serial = serial();
    let rt = Runtime::new().unwrap();

    let start = Instant::now();
    drop(rt);
    let took = start.elapsed();

    println!("dropping an idle runtime took {:?}", took);
    assert!(
        took <= Duration::from_millis(200),
        "dropping an idle runtime took {:?}",
        took
    );
}
