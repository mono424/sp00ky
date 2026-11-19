//! `SurrealVerifier` over real HTTP: against a stand-in for SurrealDB's `/rpc`
//! that records what it was asked, and, only when asked for, against a real
//! SurrealDB (`a_real_surrealdb`, ignored by default).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use serde_json::{Value, json};
use sp00ky_relay::auth::SurrealVerifier;
use sp00ky_relay::{Verifier, VerifyError};
use tokio::net::TcpListener;

/// Every request the stand-in got: its headers and its body.
type Seen = Arc<Mutex<Vec<(HeaderMap, String)>>>;

/// What SurrealDB answers a one statement query with.
fn answer(result: &str) -> String {
    format!(
        r#"{{"id":1,"result":[{{"result":{result},"status":"OK","time":"72.5µs","type":null}}]}}"#
    )
}

/// Answers by token, the way the real database was seen to (4.0.1).
async fn rpc(State(seen): State<Seen>, headers: HeaderMap, body: String) -> (StatusCode, String) {
    let token = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default()
        .to_owned();
    seen.lock().unwrap().push((headers, body));
    match token.as_str() {
        "good" => (StatusCode::OK, answer(r#"["user:abc","main","app"]"#)),
        // A valid token of another database on the same instance.
        "foreign" => (StatusCode::OK, answer(r#"["user:abc","evil","evil"]"#)),
        "root" => (StatusCode::OK, answer(r#"[null,"main","app"]"#)),
        "broken" => (StatusCode::INTERNAL_SERVER_ERROR, "boom".to_owned()),
        "slow" => {
            tokio::time::sleep(Duration::from_secs(5)).await;
            (StatusCode::OK, answer(r#"["user:abc","main","app"]"#))
        }
        _ => (
            StatusCode::UNAUTHORIZED,
            "There was a problem with authentication".to_owned(),
        ),
    }
}

async fn stand_in() -> (SocketAddr, Seen) {
    let seen = Seen::default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/rpc", post(rpc))
        .with_state(Arc::clone(&seen));
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (addr, seen)
}

#[tokio::test]
async fn the_request_is_the_one_surrealdb_expects() {
    let (addr, seen) = stand_in().await;
    let verifier = SurrealVerifier::new(&format!("ws://{addr}/rpc"), "main", "app").unwrap();
    assert_eq!(verifier.rpc_url(), format!("http://{addr}/rpc"));

    assert_eq!(verifier.verify("good").await, Ok("user:abc".to_owned()));

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    let (headers, body) = &seen[0];
    let header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    assert_eq!(header("authorization"), Some("Bearer good"));
    assert_eq!(header("surreal-ns"), Some("main"));
    assert_eq!(header("surreal-db"), Some("app"));
    assert_eq!(header("accept"), Some("application/json"));
    assert_eq!(header("content-type"), Some("application/json"));
    assert_eq!(
        serde_json::from_str::<Value>(body).unwrap(),
        json!({
            "id": 1,
            "method": "query",
            "params": ["RETURN [$auth.id, session::ns(), session::db()];", {}],
        })
    );
}

#[tokio::test]
async fn what_each_answer_means() {
    let (addr, seen) = stand_in().await;
    let verifier = SurrealVerifier::new(&format!("http://{addr}/"), "main", "app").unwrap();

    assert_eq!(verifier.verify("good").await, Ok("user:abc".to_owned()));
    assert_eq!(
        verifier.verify("foreign").await,
        Err(VerifyError::Unauthorized)
    );
    assert_eq!(
        verifier.verify("root").await,
        Err(VerifyError::Unauthorized)
    );
    assert_eq!(
        verifier.verify("made-up").await,
        Err(VerifyError::Unauthorized)
    );
    assert!(matches!(
        verifier.verify("broken").await,
        Err(VerifyError::Unavailable(_))
    ));
    assert_eq!(seen.lock().unwrap().len(), 5);

    // A token that could not even be a header value is not sent at all.
    assert_eq!(
        verifier.verify("two words").await,
        Err(VerifyError::Unauthorized)
    );
    assert_eq!(verifier.verify("").await, Err(VerifyError::Unauthorized));
    assert_eq!(seen.lock().unwrap().len(), 5);

    // The same user id is only good for the database it comes from.
    let elsewhere = SurrealVerifier::new(&format!("http://{addr}"), "main", "other").unwrap();
    assert_eq!(
        elsewhere.verify("good").await,
        Err(VerifyError::Unauthorized)
    );
}

#[tokio::test]
async fn a_database_that_does_not_answer_is_unavailable() {
    let (addr, _seen) = stand_in().await;
    let verifier = SurrealVerifier::new(&format!("http://{addr}"), "main", "app")
        .unwrap()
        .with_timeout(Duration::from_millis(200));
    let started = Instant::now();
    assert!(matches!(
        verifier.verify("slow").await,
        Err(VerifyError::Unavailable(_))
    ));
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the timeout did not apply"
    );

    // Nothing listens here any more.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gone = listener.local_addr().unwrap();
    drop(listener);
    let verifier = SurrealVerifier::new(&format!("http://{gone}"), "main", "app").unwrap();
    match verifier.verify("good").await {
        // The reason is for the relay's log, and it must not carry the token.
        Err(VerifyError::Unavailable(why)) => assert!(!why.contains("good"), "{why}"),
        other => panic!("expected unavailable, got {other:?}"),
    }
}

/// The verifier against a real SurrealDB with a `root`/`root` user:
///
/// ```sh
/// docker run --rm -p 8000:8000 surrealdb/surrealdb:v4.0.1 start --user root --pass root memory
/// SPKY_RELAY_TEST_SURREAL=http://127.0.0.1:8000 \
///     cargo test -p sp00ky-relay --test auth -- --ignored
/// ```
///
/// It makes two namespaces, `relay_test_home` and `relay_test_other`, each
/// with a database `app` whose access method signs a token for whatever record
/// id the caller asks for. That is what the owner of another database on a
/// shared instance can do, and the relay must not let such a token in.
#[tokio::test]
#[ignore = "needs a SurrealDB: set SPKY_RELAY_TEST_SURREAL to its URL (root/root)"]
async fn a_real_surrealdb() {
    let base = std::env::var("SPKY_RELAY_TEST_SURREAL")
        .expect("set SPKY_RELAY_TEST_SURREAL to the URL of a SurrealDB with a root/root user");
    let base = base.trim_end_matches('/').to_owned();
    let http = reqwest::Client::new();

    let rpc = |method: &'static str, params: Value| {
        let request = http
            .post(format!("{base}/rpc"))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .body(json!({"id": 1, "method": method, "params": [params]}).to_string());
        async move {
            let answer: Value =
                serde_json::from_str(&request.send().await.unwrap().text().await.unwrap()).unwrap();
            // A token is a plain string on 3.x and 4.x, an object with an
            // `access` member where refresh tokens are on.
            let result = &answer["result"];
            result
                .as_str()
                .or_else(|| result["access"].as_str())
                .or_else(|| result["token"].as_str())
                .unwrap_or_else(|| panic!("no token in {answer}"))
                .to_owned()
        }
    };

    for ns in ["relay_test_home", "relay_test_other"] {
        let setup = format!(
            "DEFINE NAMESPACE IF NOT EXISTS {ns}; USE NS {ns}; \
             DEFINE DATABASE IF NOT EXISTS app; USE NS {ns} DB app; \
             DEFINE TABLE OVERWRITE user SCHEMALESS PERMISSIONS FOR select WHERE id = $auth.id; \
             DEFINE ACCESS OVERWRITE account ON DATABASE TYPE RECORD \
                 SIGNUP (UPSERT type::record('user', $want) SET email = $email) \
                 DURATION FOR TOKEN 1h;"
        );
        let answer = http
            .post(format!("{base}/sql"))
            .basic_auth("root", Some("root"))
            .header("Accept", "application/json")
            .body(setup)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let statements: Value =
            serde_json::from_str(&answer).unwrap_or_else(|_| panic!("setup: {answer}"));
        for statement in statements.as_array().unwrap() {
            assert_eq!(statement["status"], "OK", "setup of {ns}: {answer}");
        }
    }

    let signup = |ns: &'static str| {
        rpc(
            "signup",
            json!({"NS": ns, "DB": "app", "AC": "account", "email": "a@example.com", "want": "victim"}),
        )
    };
    let home_token = signup("relay_test_home").await;
    let other_token = signup("relay_test_other").await;
    let root_token = rpc("signin", json!({"user": "root", "pass": "root"})).await;

    let home = SurrealVerifier::new(&base, "relay_test_home", "app").unwrap();
    let other = SurrealVerifier::new(&base, "relay_test_other", "app").unwrap();

    assert_eq!(home.verify(&home_token).await, Ok("user:victim".to_owned()));
    // A perfectly valid token for the very same record id, of another
    // database: accepted where it belongs and nowhere else.
    assert_eq!(
        other.verify(&other_token).await,
        Ok("user:victim".to_owned())
    );
    assert_eq!(
        home.verify(&other_token).await,
        Err(VerifyError::Unauthorized)
    );
    assert_eq!(
        other.verify(&home_token).await,
        Err(VerifyError::Unauthorized)
    );

    // A system user has no user id, and a made up or cut off token is no token.
    assert_eq!(
        home.verify(&root_token).await,
        Err(VerifyError::Unauthorized)
    );
    assert_eq!(
        home.verify("not-a-token").await,
        Err(VerifyError::Unauthorized)
    );
    let cut = &home_token[..home_token.len() - 4];
    assert_eq!(home.verify(cut).await, Err(VerifyError::Unauthorized));

    // The `ws://.../rpc` form the browser client is configured with.
    let ws = base.replacen("http", "ws", 1) + "/rpc";
    let as_client = SurrealVerifier::new(&ws, "relay_test_home", "app").unwrap();
    assert_eq!(
        as_client.verify(&home_token).await,
        Ok("user:victim".to_owned())
    );
}
