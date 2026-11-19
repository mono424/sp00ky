//! The relay over real sockets: a server on `127.0.0.1:0`, a fake verifier,
//! and tokio-tungstenite clients. Timers are shortened through `Config`, so
//! nothing here sleeps through a production timeout.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sp00ky_relay::{Config, Relay, Verifier, VerifyError};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Instant, sleep, timeout, timeout_at};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// How long a test waits for something that should happen at once.
const WAIT: Duration = Duration::from_secs(5);

/// Token `t-<user>` is user `user:<user>`, `down` is a database that cannot
/// be reached, anything else is nobody.
struct FakeVerifier;

#[async_trait::async_trait]
impl Verifier for FakeVerifier {
    async fn verify(&self, token: &str) -> Result<String, VerifyError> {
        match token.strip_prefix("t-") {
            Some(user) if !user.is_empty() => Ok(format!("user:{user}")),
            _ if token == "down" => Err(VerifyError::Unavailable("database down".to_owned())),
            _ => Err(VerifyError::Unauthorized),
        }
    }
}

async fn start(config: Config) -> (SocketAddr, Relay) {
    let relay = Relay::new(config, Arc::new(FakeVerifier));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = relay.router();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (addr, relay)
}

async fn connect(addr: SocketAddr) -> Ws {
    connect_async(format!("ws://{addr}/room")).await.unwrap().0
}

async fn send(ws: &mut Ws, frame: Value) {
    ws.send(Message::text(frame.to_string())).await.unwrap();
}

async fn send_raw(ws: &mut Ws, frame: &str) {
    ws.send(Message::text(frame)).await.unwrap();
}

fn hello(user: &str, peer: &str) -> Value {
    json!({"t": "hello", "v": 1, "token": format!("t-{user}"), "peer": peer})
}

/// The next text frame exactly as it came over the wire.
async fn next_raw(ws: &mut Ws) -> String {
    loop {
        match timeout(WAIT, ws.next())
            .await
            .expect("timed out waiting for a frame")
        {
            Some(Ok(Message::Text(frame))) => return frame,
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            other => panic!("expected a text frame, got {other:?}"),
        }
    }
}

async fn next(ws: &mut Ws) -> Value {
    serde_json::from_str(&next_raw(ws).await).unwrap()
}

/// Connect and say hello as `peer` of `user`. Returns the welcome.
async fn join(addr: SocketAddr, user: &str, peer: &str) -> (Ws, Value) {
    let mut ws = connect(addr).await;
    send(&mut ws, hello(user, peer)).await;
    let welcome = next(&mut ws).await;
    assert_eq!(welcome["t"], "welcome", "{welcome}");
    (ws, welcome)
}

/// The connection ends, and no text frame comes before it does.
async fn closed(ws: &mut Ws) {
    loop {
        match timeout(WAIT, ws.next())
            .await
            .expect("timed out waiting for the close")
        {
            None | Some(Err(_)) => return,
            Some(Ok(Message::Text(frame))) => panic!("unexpected frame before the close: {frame}"),
            // The Close frame itself: reading on is what answers it.
            Some(Ok(_)) => {}
        }
    }
}

/// An error frame with `code`, then the close.
async fn refused(ws: &mut Ws, code: &str) {
    let frame = next(ws).await;
    assert_eq!(frame["t"], "error", "{frame}");
    assert_eq!(frame["code"], code, "{frame}");
    assert!(frame["message"].is_string(), "{frame}");
    closed(ws).await;
}

/// No frame for `wait`. The socket is polled all the while, so the client
/// keeps answering the server's pings.
async fn silent(ws: &mut Ws, wait: Duration) {
    let deadline = Instant::now() + wait;
    loop {
        match timeout_at(deadline, ws.next()).await {
            Err(_) => return,
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
            Ok(other) => panic!("expected silence, got {other:?}"),
        }
    }
}

/// Wait for the relay to hold exactly this many rooms and members. Cleanup
/// runs in the connection's own task, a moment after the socket closes.
async fn holds(relay: &Relay, rooms: usize, peers: usize) {
    let deadline = Instant::now() + WAIT;
    while (relay.room_count(), relay.peer_count()) != (rooms, peers) {
        assert!(
            Instant::now() < deadline,
            "expected {rooms} rooms and {peers} peers, the relay holds {} and {}",
            relay.room_count(),
            relay.peer_count(),
        );
        sleep(Duration::from_millis(10)).await;
    }
}

fn msg(from: &str, d: Value) -> Value {
    json!({"t": "msg", "from": from, "d": d})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn welcome_lists_the_room_and_the_others_see_the_join() {
    let (addr, relay) = start(Config::default()).await;

    let (mut a, welcome) = join(addr, "ann", "a").await;
    assert_eq!(
        welcome,
        json!({"t": "welcome", "user": "user:ann", "peer": "a", "peers": []})
    );

    let (mut b, welcome) = join(addr, "ann", "b").await;
    assert_eq!(
        welcome,
        json!({"t": "welcome", "user": "user:ann", "peer": "b", "peers": ["a"]})
    );
    assert_eq!(next(&mut a).await, json!({"t": "join", "peer": "b"}));

    let (_c, welcome) = join(addr, "ann", "c").await;
    assert_eq!(welcome["peers"], json!(["a", "b"]));
    assert_eq!(next(&mut a).await, json!({"t": "join", "peer": "c"}));
    assert_eq!(next(&mut b).await, json!({"t": "join", "peer": "c"}));

    holds(&relay, 1, 3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn msg_goes_to_all_or_to_the_named_peers_and_never_back() {
    let (addr, _relay) = start(Config::default()).await;
    let (mut a, _) = join(addr, "ann", "a").await;
    let (mut b, _) = join(addr, "ann", "b").await;
    let (mut c, _) = join(addr, "ann", "c").await;
    assert_eq!(next(&mut a).await["peer"], "b");
    assert_eq!(next(&mut a).await["peer"], "c");
    assert_eq!(next(&mut b).await["peer"], "c");

    // No `to`: every other member.
    send(&mut a, json!({"t": "msg", "d": {"n": 1}})).await;
    assert_eq!(next(&mut b).await, msg("a", json!({"n": 1})));
    assert_eq!(next(&mut c).await, msg("a", json!({"n": 1})));

    // `to: null` is the same.
    send(&mut a, json!({"t": "msg", "to": null, "d": {"n": 2}})).await;
    assert_eq!(next(&mut b).await, msg("a", json!({"n": 2})));
    assert_eq!(next(&mut c).await, msg("a", json!({"n": 2})));

    // A subset. Frames from one sender arrive in order, so the next thing b
    // sees being n 5 proves it got neither 3 nor 4.
    send(&mut a, json!({"t": "msg", "to": ["c"], "d": {"n": 3}})).await;
    // Named twice is delivered once, unknown peers and the sender itself are
    // skipped without a word.
    send(
        &mut a,
        json!({"t": "msg", "to": ["c", "ghost", "a", "c"], "d": {"n": 4}}),
    )
    .await;
    // An empty list is nobody.
    send(&mut a, json!({"t": "msg", "to": [], "d": {"n": 0}})).await;
    send(&mut a, json!({"t": "msg", "to": ["b", "c"], "d": {"n": 5}})).await;
    assert_eq!(next(&mut b).await, msg("a", json!({"n": 5})));
    assert_eq!(next(&mut c).await, msg("a", json!({"n": 3})));
    assert_eq!(next(&mut c).await, msg("a", json!({"n": 4})));
    assert_eq!(next(&mut c).await, msg("a", json!({"n": 5})));

    // Nothing of all that came back to a: its next frame is b's.
    send(&mut b, json!({"t": "msg", "to": ["a"], "d": "from b"})).await;
    assert_eq!(next(&mut a).await, msg("b", json!("from b")));
    silent(&mut c, Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_payload_is_forwarded_byte_for_byte() {
    let (addr, _relay) = start(Config::default()).await;
    let (mut a, _) = join(addr, "ann", "a").await;
    let (mut b, _) = join(addr, "ann", "b").await;
    assert_eq!(next(&mut a).await["peer"], "b");

    // Odd spacing, key order, escapes left as escapes, raw unicode, and a
    // number no float can hold: all of it has to arrive untouched.
    let payloads = [
        r#"{"op":"update","row" : {"id":"thread:1","title":"Grüße 😀","tags":[ 1,2.50 ,{"deep":[[]]}]},  "z":null}"#,
        r#"{"escaped":"é😀 \" \\ \/ \n","big":123456789012345678901234567890,"exp":1E400}"#,
        r#"{"b":1,"a":2,"b":3}"#,
        "null",
        "true",
        "0",
        "-1.50",
        r#""just a string""#,
        "[]",
        "{}",
        r#"[ "ü" , {"k" :"v"} ]"#,
    ];
    for payload in payloads {
        send_raw(&mut a, &format!(r#"{{"t":"msg","d":{payload}}}"#)).await;
        assert_eq!(
            next_raw(&mut b).await,
            format!(r#"{{"t":"msg","from":"a","d":{payload}}}"#)
        );
    }

    // Whatever surrounds the value in the sender's frame stays behind.
    send_raw(
        &mut a,
        "{ \"d\" :  {\"k\" : 1}  , \"to\":[\"b\"],\n \"t\" : \"msg\" }",
    )
    .await;
    assert_eq!(
        next_raw(&mut b).await,
        r#"{"t":"msg","from":"a","d":{"k" : 1}}"#
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_users_never_see_each_other() {
    let (addr, relay) = start(Config::default()).await;
    // The same peer ids in both rooms, so only the room keeps them apart.
    let (mut ann_x, _) = join(addr, "ann", "x").await;
    let (mut bob_x, welcome) = join(addr, "bob", "x").await;
    assert_eq!(
        welcome,
        json!({"t": "welcome", "user": "user:bob", "peer": "x", "peers": []})
    );
    let (mut ann_y, welcome) = join(addr, "ann", "y").await;
    assert_eq!(welcome["peers"], json!(["x"]));
    let (mut bob_y, welcome) = join(addr, "bob", "y").await;
    assert_eq!(welcome["peers"], json!(["x"]));
    assert_eq!(next(&mut ann_x).await, json!({"t": "join", "peer": "y"}));
    assert_eq!(next(&mut bob_x).await, json!({"t": "join", "peer": "y"}));
    holds(&relay, 2, 4).await;

    send(&mut ann_x, json!({"t": "msg", "d": "ann to all"})).await;
    send(
        &mut ann_x,
        json!({"t": "msg", "to": ["y"], "d": "ann to y"}),
    )
    .await;
    assert_eq!(next(&mut ann_y).await, msg("x", json!("ann to all")));
    assert_eq!(next(&mut ann_y).await, msg("x", json!("ann to y")));

    // Bob's devices saw none of it: the first frames they get are their own.
    send(&mut bob_x, json!({"t": "msg", "d": "bob to all"})).await;
    assert_eq!(next(&mut bob_y).await, msg("x", json!("bob to all")));
    send(&mut bob_y, json!({"t": "msg", "d": "bob back"})).await;
    assert_eq!(next(&mut bob_x).await, msg("y", json!("bob back")));

    // And a leave stays in its room too.
    ann_y.close(None).await.unwrap();
    assert_eq!(next(&mut ann_x).await, json!({"t": "leave", "peer": "y"}));
    silent(&mut bob_x, Duration::from_millis(100)).await;
    silent(&mut bob_y, Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_disconnect_is_a_leave_and_an_empty_room_is_forgotten() {
    let (addr, relay) = start(Config::default()).await;
    let (mut a, _) = join(addr, "ann", "a").await;
    let (mut b, _) = join(addr, "ann", "b").await;
    let (c, _) = join(addr, "ann", "c").await;
    assert_eq!(next(&mut a).await["peer"], "b");
    assert_eq!(next(&mut a).await["peer"], "c");
    holds(&relay, 1, 3).await;

    // A clean close.
    b.close(None).await.unwrap();
    assert_eq!(next(&mut a).await, json!({"t": "leave", "peer": "b"}));
    holds(&relay, 1, 2).await;

    // A socket that just goes away.
    drop(c);
    assert_eq!(next(&mut a).await, json!({"t": "leave", "peer": "c"}));
    holds(&relay, 1, 1).await;

    drop(a);
    holds(&relay, 0, 0).await;

    // The next device starts a fresh room.
    let (_a, welcome) = join(addr, "ann", "a").await;
    assert_eq!(welcome["peers"], json!([]));
    holds(&relay, 1, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hello_that_is_refused() {
    let (addr, relay) = start(Config::default()).await;
    let (mut member, _) = join(addr, "ann", "member").await;

    let cases = [
        // Tokens the verifier does not know, and no token worth asking about.
        (
            json!({"t": "hello", "v": 1, "token": "stolen", "peer": "p"}),
            "unauthorized",
        ),
        (
            json!({"t": "hello", "v": 1, "token": "", "peer": "p"}),
            "unauthorized",
        ),
        (
            json!({"t": "hello", "v": 1, "token": "down", "peer": "p"}),
            "unavailable",
        ),
        // Malformed.
        (json!({"t": "hello", "v": 1, "peer": "p"}), "bad_request"),
        (
            json!({"t": "hello", "v": 1, "token": "t-ann"}),
            "bad_request",
        ),
        (
            json!({"t": "hello", "token": "t-ann", "peer": "p"}),
            "bad_request",
        ),
        (
            json!({"t": "hello", "v": 2, "token": "t-ann", "peer": "p"}),
            "bad_request",
        ),
        (
            json!({"t": "hello", "v": "1", "token": "t-ann", "peer": "p"}),
            "bad_request",
        ),
        (
            json!({"v": 1, "token": "t-ann", "peer": "p"}),
            "bad_request",
        ),
        (json!(["hello"]), "bad_request"),
        // Anything but a hello first.
        (json!({"t": "ping"}), "bad_request"),
        (json!({"t": "msg", "d": 1}), "bad_request"),
        // Peer ids.
        (
            json!({"t": "hello", "v": 1, "token": "t-ann", "peer": ""}),
            "bad_request",
        ),
        (
            json!({"t": "hello", "v": 1, "token": "t-ann", "peer": "a".repeat(65)}),
            "bad_request",
        ),
        (
            json!({"t": "hello", "v": 1, "token": "t-ann", "peer": "a b"}),
            "bad_request",
        ),
        (
            json!({"t": "hello", "v": 1, "token": "t-ann", "peer": "a\"b"}),
            "bad_request",
        ),
        (
            json!({"t": "hello", "v": 1, "token": "t-ann", "peer": "é"}),
            "bad_request",
        ),
        (
            json!({"t": "hello", "v": 1, "token": "t-ann", "peer": 7}),
            "bad_request",
        ),
    ];
    for (frame, code) in cases {
        let mut ws = connect(addr).await;
        send(&mut ws, frame.clone()).await;
        let error = next(&mut ws).await;
        assert_eq!(error["t"], "error", "{frame}");
        assert_eq!(error["code"], code, "{frame}");
        closed(&mut ws).await;
    }

    let mut ws = connect(addr).await;
    send_raw(&mut ws, "this is not json").await;
    refused(&mut ws, "bad_request").await;

    let mut ws = connect(addr).await;
    ws.send(Message::binary(hello("ann", "p").to_string().into_bytes()))
        .await
        .unwrap();
    refused(&mut ws, "bad_request").await;

    // A peer id of exactly 64 characters is fine.
    let longest = "a".repeat(64);
    let (_ws, welcome) = join(addr, "ann", &longest).await;
    assert_eq!(welcome["peer"], longest.as_str());
    assert_eq!(
        next(&mut member).await,
        json!({"t": "join", "peer": longest})
    );

    // None of the refused ones ever reached the room.
    silent(&mut member, Duration::from_millis(100)).await;
    holds(&relay, 1, 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_says_nothing_is_closed() {
    let config = Config {
        hello_timeout: Duration::from_millis(200),
        ..Config::default()
    };
    let (addr, relay) = start(config).await;

    let started = Instant::now();
    let mut ws = connect(addr).await;
    refused(&mut ws, "bad_request").await;
    assert!(started.elapsed() >= Duration::from_millis(200));
    holds(&relay, 0, 0).await;

    // A hello in time is not affected, and the timeout is over once it is in.
    let (mut ws, _) = join(addr, "ann", "a").await;
    silent(&mut ws, Duration::from_millis(400)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_peer_id_again_replaces_the_old_connection() {
    let (addr, relay) = start(Config::default()).await;
    let (mut old, _) = join(addr, "ann", "a").await;
    let (mut b, _) = join(addr, "ann", "b").await;
    assert_eq!(next(&mut old).await, json!({"t": "join", "peer": "b"}));

    let (mut new, welcome) = join(addr, "ann", "a").await;
    assert_eq!(
        welcome,
        json!({"t": "welcome", "user": "user:ann", "peer": "a", "peers": ["b"]})
    );
    refused(&mut old, "replaced").await;

    // The old connection ending must not take the seat with it.
    holds(&relay, 1, 2).await;
    sleep(Duration::from_millis(100)).await;
    holds(&relay, 1, 2).await;

    // b saw neither a leave nor a join: the next frame it gets is a message
    // from the new connection.
    send(&mut new, json!({"t": "msg", "d": "new a"})).await;
    assert_eq!(next(&mut b).await, msg("a", json!("new a")));
    send(&mut b, json!({"t": "msg", "to": ["a"], "d": "to a"})).await;
    assert_eq!(next(&mut new).await, msg("b", json!("to a")));

    // The seat is the new connection's: when that one goes, b is told.
    new.close(None).await.unwrap();
    assert_eq!(next(&mut b).await, json!({"t": "leave", "peer": "a"}));
    holds(&relay, 1, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_room_refuses_a_new_peer_but_not_a_replacement() {
    let config = Config {
        max_room_peers: 2,
        ..Config::default()
    };
    let (addr, relay) = start(config).await;
    let (mut a, _) = join(addr, "ann", "a").await;
    let (mut b, _) = join(addr, "ann", "b").await;
    assert_eq!(next(&mut a).await, json!({"t": "join", "peer": "b"}));

    let mut c = connect(addr).await;
    send(&mut c, hello("ann", "c")).await;
    refused(&mut c, "room_full").await;
    holds(&relay, 1, 2).await;

    // Another user's room has its own seats.
    let (_bob, _) = join(addr, "bob", "c").await;
    holds(&relay, 2, 3).await;

    // Replacing a member of the full room is not an extra member.
    let (mut a2, welcome) = join(addr, "ann", "a").await;
    assert_eq!(welcome["peers"], json!(["b"]));
    refused(&mut a, "replaced").await;

    // b heard nothing of the refused hello or the replacement.
    send(&mut a2, json!({"t": "msg", "d": 1})).await;
    assert_eq!(next(&mut b).await, msg("a", json!(1)));

    // A seat that frees up can be taken.
    b.close(None).await.unwrap();
    assert_eq!(next(&mut a2).await, json!({"t": "leave", "peer": "b"}));
    let (_c, welcome) = join(addr, "ann", "c").await;
    assert_eq!(welcome["peers"], json!(["a"]));
    holds(&relay, 2, 3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oversized_frame_closes_the_connection() {
    let config = Config {
        max_frame_bytes: 1024,
        ..Config::default()
    };
    let (addr, relay) = start(config).await;
    let (mut b, _) = join(addr, "ann", "b").await;
    let (mut a, _) = join(addr, "ann", "a").await;
    assert_eq!(next(&mut b).await, json!({"t": "join", "peer": "a"}));

    // Under the limit goes through.
    let fits = "x".repeat(900);
    send(&mut a, json!({"t": "msg", "d": fits})).await;
    assert_eq!(next(&mut b).await, msg("a", json!(fits)));

    send(&mut a, json!({"t": "msg", "d": "x".repeat(4000)})).await;
    closed(&mut a).await;
    // Nothing of the big frame was forwarded.
    assert_eq!(next(&mut b).await, json!({"t": "leave", "peer": "a"}));
    holds(&relay, 1, 1).await;

    // The same holds before the hello.
    let mut ws = connect(addr).await;
    send(
        &mut ws,
        json!({"t": "hello", "v": 1, "token": "t-ann", "peer": "x".repeat(4000)}),
    )
    .await;
    closed(&mut ws).await;
    holds(&relay, 1, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frames_over_the_rate_limit_are_dropped_and_then_the_connection_is_closed() {
    // No refill, so exactly the burst gets through.
    let config = Config {
        rate_burst: 5,
        rate_per_sec: 0,
        rate_drop_limit: 10,
        ..Config::default()
    };
    let (addr, relay) = start(config).await;
    let (mut b, _) = join(addr, "ann", "b").await;
    let (mut a, _) = join(addr, "ann", "a").await;
    assert_eq!(next(&mut b).await, json!({"t": "join", "peer": "a"}));

    // 5 pass, 9 are dropped: one short of the limit, the connection stays.
    for n in 0..14 {
        send(&mut a, json!({"t": "msg", "d": n})).await;
    }
    for n in 0..5 {
        assert_eq!(next(&mut b).await, msg("a", json!(n)));
    }
    silent(&mut b, Duration::from_millis(150)).await;
    silent(&mut a, Duration::from_millis(50)).await;
    holds(&relay, 1, 2).await;

    // The tenth dropped frame in a row closes it.
    send(&mut a, json!({"t": "msg", "d": 14})).await;
    refused(&mut a, "rate_limited").await;
    assert_eq!(next(&mut b).await, json!({"t": "leave", "peer": "a"}));
    holds(&relay, 1, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stuck_member_is_disconnected_and_the_others_carry_on() {
    let config = Config {
        queue_frames: 8,
        rate_burst: u32::MAX,
        rate_per_sec: u32::MAX,
        ..Config::default()
    };
    let (addr, relay) = start(config).await;
    // Never read from again: the kernel buffers fill, the relay's writer for
    // this member blocks, and then its queue fills.
    let (mut stuck, _) = join(addr, "ann", "stuck").await;
    let (mut fast, _) = join(addr, "ann", "fast").await;
    let (mut sender, _) = join(addr, "ann", "sender").await;
    assert_eq!(
        next(&mut fast).await,
        json!({"t": "join", "peer": "sender"})
    );

    let filler = "x".repeat(32_000);
    let mut sent = 0u32;
    let mut left = false;
    while !left {
        assert!(sent < 5_000, "the stuck member was never disconnected");
        send(
            &mut sender,
            json!({"t": "msg", "d": {"n": sent, "filler": filler}}),
        )
        .await;
        // One frame at a time, so the fast member's queue never holds more
        // than a frame or two. It gets every message, and at some point the
        // leave of the stuck one.
        loop {
            let frame = next(&mut fast).await;
            if frame["t"] == "leave" {
                assert_eq!(frame["peer"], "stuck");
                left = true;
                continue;
            }
            assert_eq!(frame["from"], "sender");
            assert_eq!(frame["d"]["n"], sent);
            break;
        }
        sent += 1;
    }

    // The sender was told too, and was never held up.
    assert_eq!(
        next(&mut sender).await,
        json!({"t": "leave", "peer": "stuck"})
    );
    holds(&relay, 1, 2).await;
    send(&mut sender, json!({"t": "msg", "d": "after"})).await;
    assert_eq!(next(&mut fast).await, msg("sender", json!("after")));

    // The stuck socket was dropped: what was buffered can still be read, and
    // then it ends.
    let drained = timeout(Duration::from_secs(30), async {
        while let Some(Ok(_)) = stuck.next().await {}
    })
    .await;
    assert!(drained.is_ok(), "the stuck connection was never closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_that_is_too_old_is_told_to_reauth() {
    let config = Config {
        session_max: Duration::from_millis(300),
        ..Config::default()
    };
    let (addr, relay) = start(config).await;
    let started = Instant::now();
    let (mut a, _) = join(addr, "ann", "a").await;

    // Traffic does not extend a session.
    for _ in 0..3 {
        send(&mut a, json!({"t": "ping"})).await;
        assert_eq!(next(&mut a).await, json!({"t": "pong"}));
        sleep(Duration::from_millis(50)).await;
    }
    refused(&mut a, "reauth").await;
    assert!(started.elapsed() >= Duration::from_millis(300));
    holds(&relay, 0, 0).await;

    // A reconnect starts a new session.
    let (_a, welcome) = join(addr, "ann", "a").await;
    assert_eq!(welcome["peers"], json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_answers_pong() {
    let (addr, _relay) = start(Config::default()).await;
    let (mut a, _) = join(addr, "ann", "a").await;
    let (mut b, _) = join(addr, "ann", "b").await;
    assert_eq!(next(&mut a).await["peer"], "b");

    send(&mut a, json!({"t": "ping"})).await;
    assert_eq!(next_raw(&mut a).await, r#"{"t":"pong"}"#);
    // Only the one who asked is answered.
    silent(&mut b, Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_outside_the_protocol_closes_the_connection() {
    let (addr, relay) = start(Config::default()).await;
    let (mut observer, _) = join(addr, "ann", "observer").await;

    let bad = [
        // A hello after the welcome.
        Message::text(hello("ann", "again").to_string()),
        Message::text(json!({"t": "subscribe"}).to_string()),
        Message::text(json!({"d": 1}).to_string()),
        Message::text(json!({"t": "msg"}).to_string()),
        Message::text(json!({"t": "msg", "to": "observer", "d": 1}).to_string()),
        Message::text(json!({"t": "msg", "to": [1], "d": 1}).to_string()),
        Message::text("not json"),
        Message::text(r#"{"t":"msg","d":}"#),
        Message::binary(json!({"t": "msg", "d": 1}).to_string().into_bytes()),
    ];
    for (n, frame) in bad.into_iter().enumerate() {
        let peer = format!("p{n}");
        let (mut ws, _) = join(addr, "ann", &peer).await;
        assert_eq!(
            next(&mut observer).await,
            json!({"t": "join", "peer": peer})
        );
        ws.send(frame.clone()).await.unwrap();
        let error = next(&mut ws).await;
        assert_eq!(error["code"], "bad_request", "{frame:?}");
        closed(&mut ws).await;
        // Nothing of the bad frame reached the room, only the leave.
        assert_eq!(
            next(&mut observer).await,
            json!({"t": "leave", "peer": peer}),
            "{frame:?}"
        );
    }
    holds(&relay, 1, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_connection_is_closed_and_one_that_answers_pings_is_not() {
    let config = Config {
        ping_interval: Duration::from_millis(50),
        idle_timeout: Duration::from_millis(300),
        ..Config::default()
    };
    let (addr, relay) = start(config).await;
    let (mut alive, _) = join(addr, "ann", "alive").await;
    // Never polled again, so it never answers a ping.
    let before = Instant::now();
    let (mut dead, _) = join(addr, "ann", "dead").await;
    assert_eq!(next(&mut alive).await, json!({"t": "join", "peer": "dead"}));

    // The server does ping.
    let ping = timeout(WAIT, async {
        match alive.next().await {
            Some(Ok(Message::Ping(_))) => {}
            other => panic!("expected a ping, got {other:?}"),
        }
    })
    .await;
    assert!(ping.is_ok(), "the server never sent a ping");

    // Reading is what answers the pings, and `next` keeps reading.
    assert_eq!(
        next(&mut alive).await,
        json!({"t": "leave", "peer": "dead"})
    );
    assert!(before.elapsed() >= Duration::from_millis(300));
    holds(&relay, 1, 1).await;

    // Well past its own idle timeout the live one is still there, kept alive
    // by nothing but pongs.
    silent(&mut alive, Duration::from_millis(600)).await;
    send(&mut alive, json!({"t": "ping"})).await;
    assert_eq!(next(&mut alive).await, json!({"t": "pong"}));

    closed(&mut dead).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_answers_ok() {
    let (addr, _relay) = start(Config::default()).await;
    let response = reqwest::get(format!("http://{addr}/health")).await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "ok");

    // `/room` is a WebSocket and nothing else.
    let response = reqwest::get(format!("http://{addr}/room")).await.unwrap();
    assert!(response.status().is_client_error(), "{}", response.status());
    let response = reqwest::get(format!("http://{addr}/")).await.unwrap();
    assert_eq!(response.status(), 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_origin_allowlist_refuses_other_origins() {
    let config = Config {
        allowed_origins: vec!["https://app.example.com".to_owned()],
        ..Config::default()
    };
    let (addr, relay) = start(config).await;
    let with_origin = |origin: &'static str| {
        let mut request = format!("ws://{addr}/room").into_client_request().unwrap();
        request
            .headers_mut()
            .insert("Origin", HeaderValue::from_static(origin));
        request
    };

    match connect_async(with_origin("https://evil.example")).await {
        Err(WsError::Http(response)) => assert_eq!(response.status(), 403),
        other => panic!("expected a 403, got {other:?}"),
    }
    holds(&relay, 0, 0).await;

    let (mut ws, _) = connect_async(with_origin("https://app.example.com"))
        .await
        .unwrap();
    send(&mut ws, hello("ann", "web")).await;
    assert_eq!(next(&mut ws).await["t"], "welcome");

    // No Origin at all is a native client, which the list is not about.
    let (_native, welcome) = join(addr, "ann", "native").await;
    assert_eq!(welcome["peers"], json!(["web"]));

    // Without a list every origin may upgrade.
    let (open, _relay) = start(Config::default()).await;
    let mut request = format!("ws://{open}/room").into_client_request().unwrap();
    request
        .headers_mut()
        .insert("Origin", HeaderValue::from_static("https://evil.example"));
    assert!(connect_async(request).await.is_ok());
}
