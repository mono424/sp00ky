//! The real [`Verifier`]: it asks SurrealDB, as the caller, who the caller is.
//!
//! One HTTP request per hello, the same wire `ssp-node`'s `HttpSqlDb` uses:
//! `POST {base}/rpc` with a JSON-RPC `query` body, the client's token as
//! `Authorization: Bearer`, and the `surreal-ns` and `surreal-db` headers.
//! `Accept: application/json` makes a record id come back as a plain string
//! such as `"user:abc"`. The relay holds no credential of its own: a token the
//! database does not accept simply gets no answer.
//!
//! The query is `RETURN [$auth.id, session::ns(), session::db()];`, and the
//! namespace and database in the answer have to be the configured ones. That
//! check is the point: SurrealDB takes the namespace and database of a session
//! from the token, not from the two headers, so `RETURN $auth.id;` alone
//! accepts a token of ANY database on the same instance. Whoever owns another
//! database there (a second tenant on a shared instance) can define an access
//! method that signs a token for any record id, a victim's `user:abc`
//! included, and would be let into the victim's room. Seen on SurrealDB 3.3.5
//! and 4.0.1.
//!
//! `$auth.id` is the user id the rest of sp00ky keys its permissions on. The
//! two server lines read it differently: 4.0.1 answers the record id of the
//! token as it is, 3.3.5 reads the record with the user's own permissions and
//! answers NONE when the user may not select its own record or the record was
//! deleted. So on 3.x a user table has to let a user select itself, and on 4.x
//! a deleted account keeps its room until its token expires, which gives it
//! nobody to talk to but its own devices.
//!
//! The parts that decide anything (the URL and what a response means) are pure
//! functions, tested without a server.

use std::time::Duration;

use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde_json::Value;

use crate::{Verifier, VerifyError};

/// The whole request body. Nothing in it depends on the caller.
const QUERY: &str =
    r#"{"id":1,"method":"query","params":["RETURN [$auth.id, session::ns(), session::db()];",{}]}"#;

/// A hello waits for this answer, so a database that hangs must not hold it
/// for long.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Session tokens are JWTs of a few hundred bytes. Anything far larger is not
/// one, and is not worth sending to the database as a header.
const MAX_TOKEN_BYTES: usize = 8192;

pub struct SurrealVerifier {
    http: reqwest::Client,
    rpc_url: String,
    ns: String,
    db: String,
    timeout: Duration,
}

impl SurrealVerifier {
    /// `url` is the database endpoint as any sp00ky service is given it:
    /// `ws://`, `wss://`, `http://` or `https://`, with or without a trailing
    /// `/` or `/rpc`.
    pub fn new(url: &str, ns: impl Into<String>, db: impl Into<String>) -> Result<Self, String> {
        let rpc_url = rpc_url(url)?;
        let http = reqwest::Client::builder()
            .build()
            .map_err(|error| format!("could not build the HTTP client: {error}"))?;
        Ok(Self {
            http,
            rpc_url,
            ns: ns.into(),
            db: db.into(),
            timeout: DEFAULT_TIMEOUT,
        })
    }

    /// How long one answer may take, 10 s unless set. Running out of it is
    /// `Unavailable`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The endpoint the tokens are checked against.
    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }
}

#[async_trait::async_trait]
impl Verifier for SurrealVerifier {
    async fn verify(&self, token: &str) -> Result<String, VerifyError> {
        if !plausible_token(token) {
            return Err(VerifyError::Unauthorized);
        }
        // `bearer_auth` marks the header as sensitive, so it stays out of any
        // debug output. The token is never logged here either: the errors
        // below carry the URL and the fault, nothing of the request.
        let response = self
            .http
            .post(&self.rpc_url)
            .bearer_auth(token)
            .header("surreal-ns", &self.ns)
            .header("surreal-db", &self.db)
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .timeout(self.timeout)
            .body(QUERY)
            .send()
            .await
            .map_err(|error| VerifyError::Unavailable(describe(&error)))?;
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .map_err(|error| VerifyError::Unavailable(describe(&error)))?;
        parse_auth_response(status, &body, &self.ns, &self.db)
    }
}

/// A token that can go into a header and could be a session token: visible
/// ASCII without spaces, of a sane length. Anything else is refused without a
/// round trip.
fn plausible_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= MAX_TOKEN_BYTES
        && token.bytes().all(|byte| byte.is_ascii_graphic())
}

/// The error and what caused it, which is where reqwest keeps the reason
/// ("connection refused", "operation timed out").
fn describe(error: &reqwest::Error) -> String {
    let mut text = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

/// Turn the configured database URL into its HTTP RPC endpoint,
/// `http(s)://host[:port]/rpc`. The browser client connects to the same
/// database over `ws(s)://.../rpc`, so the relay accepts that form too.
pub fn rpc_url(raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    let Some((scheme, rest)) = raw.split_once("://") else {
        return Err(format!(
            "`{raw}` is not a database URL, expected ws://, wss://, http:// or https://"
        ));
    };
    let scheme = match scheme.to_ascii_lowercase().as_str() {
        "ws" | "http" => "http",
        "wss" | "https" => "https",
        other => {
            return Err(format!(
                "unsupported database URL scheme `{other}`, expected ws, wss, http or https"
            ));
        }
    };
    let rest = rest.trim_end_matches('/');
    let rest = rest
        .strip_suffix("/rpc")
        .unwrap_or(rest)
        .trim_end_matches('/');
    if rest.is_empty() || rest.starts_with('/') {
        return Err(format!("`{raw}` has no host"));
    }
    let url = format!("{scheme}://{rest}/rpc");
    // Catches what the string handling above cannot (spaces, a bad port).
    reqwest::Url::parse(&url).map_err(|error| format!("`{raw}` is not a valid URL: {error}"))?;
    Ok(url)
}

/// What SurrealDB's answer to the query says about the token: the user it
/// belongs to, when that user is one of namespace `ns` and database `db`.
///
/// `Unauthorized` only when the database itself said so, because a client
/// drops its session on it. Everything that merely failed to produce an answer
/// is `Unavailable`, so a broken relay configuration or a proxy error page
/// never signs anyone out.
pub fn parse_auth_response(
    status: u16,
    body: &str,
    ns: &str,
    db: &str,
) -> Result<String, VerifyError> {
    if status == 401 || status == 403 {
        return Err(VerifyError::Unauthorized);
    }
    if !(200..300).contains(&status) {
        return Err(VerifyError::Unavailable(format!(
            "the database answered HTTP {status}"
        )));
    }
    let Ok(parsed) = serde_json::from_str::<Value>(body) else {
        return Err(VerifyError::Unavailable(
            "the database answer is not JSON".to_owned(),
        ));
    };
    // A JSON-RPC error on a query this simple is the token being refused.
    if parsed.get("error").is_some() {
        return Err(VerifyError::Unauthorized);
    }
    let Some(statement) = parsed
        .get("result")
        .and_then(Value::as_array)
        .and_then(|all| all.first())
    else {
        return Err(VerifyError::Unavailable(
            "the database answer has no result".to_owned(),
        ));
    };
    if statement.get("status").and_then(Value::as_str) != Some("OK") {
        return Err(VerifyError::Unauthorized);
    }
    let Some([user, session_ns, session_db]) = statement
        .get("result")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
    else {
        return Err(VerifyError::Unauthorized);
    };
    // A system user and an anonymous session have no `$auth.id`: it is null,
    // and neither has a room.
    let Some(user) = user.as_str().filter(|user| !user.is_empty()) else {
        return Err(VerifyError::Unauthorized);
    };
    // The token is valid, but for another database of this instance. Its user
    // id means nothing here, see the module comment.
    if session_ns.as_str() != Some(ns) || session_db.as_str() != Some(db) {
        tracing::warn!(
            token_ns = %session_ns,
            token_db = %session_db,
            ns,
            db,
            "refused a token of another database",
        );
        return Err(VerifyError::Unauthorized);
    }
    Ok(user.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_url_forms() {
        for raw in [
            "ws://db.internal:8000",
            "ws://db.internal:8000/",
            "ws://db.internal:8000/rpc",
            "ws://db.internal:8000/rpc/",
            "http://db.internal:8000",
            "http://db.internal:8000/rpc",
            "  HTTP://db.internal:8000//  ",
        ] {
            assert_eq!(
                rpc_url(raw).as_deref(),
                Ok("http://db.internal:8000/rpc"),
                "{raw}"
            );
        }
        for raw in [
            "wss://db.example.com",
            "wss://db.example.com/rpc",
            "https://db.example.com/",
            "WSS://db.example.com",
        ] {
            assert_eq!(
                rpc_url(raw).as_deref(),
                Ok("https://db.example.com/rpc"),
                "{raw}"
            );
        }
        // A path in front of the endpoint is kept.
        assert_eq!(
            rpc_url("https://example.com/db/rpc").as_deref(),
            Ok("https://example.com/db/rpc")
        );
        assert_eq!(
            rpc_url("http://[::1]:8000").as_deref(),
            Ok("http://[::1]:8000/rpc")
        );
    }

    #[test]
    fn rpc_url_refuses_what_it_cannot_use() {
        for raw in [
            "",
            "db.internal:8000",
            "ftp://db.internal",
            "http://",
            "ws:///rpc",
            "http://host:notaport",
        ] {
            assert!(rpc_url(raw).is_err(), "accepted: {raw}");
        }
    }

    /// The answer to a statement whose result is `result`.
    fn answer(result: &str) -> String {
        format!(
            r#"{{"id":1,"result":[{{"result":{result},"status":"OK","time":"72.5µs","type":null}}]}}"#
        )
    }

    fn parse(status: u16, body: &str) -> Result<String, VerifyError> {
        parse_auth_response(status, body, "main", "app")
    }

    #[test]
    fn a_record_user_is_returned() {
        assert_eq!(
            parse(200, &answer(r#"["user:abc","main","app"]"#)),
            Ok("user:abc".to_owned())
        );
    }

    #[test]
    fn a_token_of_another_database_is_refused() {
        // Valid tokens, each for a user id that may well exist here too.
        for result in [
            r#"["user:abc","evil","evil"]"#,
            r#"["user:abc","main","other"]"#,
            r#"["user:abc","other","app"]"#,
            r#"["user:abc","app","main"]"#,
            r#"["user:abc","MAIN","app"]"#,
            r#"["user:abc",null,null]"#,
            r#"["user:abc","main",null]"#,
            r#"["user:abc","main"]"#,
            r#"["user:abc"]"#,
            // The bare id, which is what `RETURN $auth.id;` would answer.
            r#""user:abc""#,
        ] {
            assert_eq!(
                parse(200, &answer(result)),
                Err(VerifyError::Unauthorized),
                "{result}"
            );
        }
    }

    #[test]
    fn the_database_refusing_is_unauthorized() {
        let unauthorized = Err(VerifyError::Unauthorized);
        assert_eq!(
            parse(401, "There was a problem with authentication"),
            unauthorized
        );
        assert_eq!(parse(403, ""), unauthorized);
        // A JSON-RPC error, as an anonymous session gets on 4.0.1.
        let error = r#"{"error":{"code":-32002,"kind":"NotAllowed","message":"Anonymous access not allowed"},"id":1}"#;
        assert_eq!(parse(200, error), unauthorized);
        // The statement failed.
        let failed = r#"{"id":1,"result":[{"status":"ERR","result":"Not allowed"}]}"#;
        assert_eq!(parse(200, failed), unauthorized);
        // No `$auth.id`: a root session, an anonymous one, a deleted user.
        for id in [
            "null",
            r#""""#,
            "{}",
            "[]",
            "7",
            r#"{"tb":"user","id":"abc"}"#,
        ] {
            let result = format!(r#"[{id},"main","app"]"#);
            assert_eq!(parse(200, &answer(&result)), unauthorized, "{result}");
        }
        assert_eq!(parse(200, &answer("null")), unauthorized);
        assert_eq!(
            parse(200, r#"{"id":1,"result":[{"status":"OK"}]}"#),
            unauthorized
        );
    }

    #[test]
    fn no_answer_is_unavailable() {
        let unavailable = |answer| matches!(answer, Err(VerifyError::Unavailable(_)));
        assert!(unavailable(parse(500, "")));
        assert!(unavailable(parse(502, "<html>Bad Gateway</html>")));
        assert!(unavailable(parse(503, "")));
        // Not the database's verdict on the token: a wrong path, a proxy.
        assert!(unavailable(parse(404, "Not Found")));
        assert!(unavailable(parse(429, "")));
        assert!(unavailable(parse(200, "<html>captive portal</html>")));
        assert!(unavailable(parse(200, r#"{"id":1}"#)));
        assert!(unavailable(parse(200, r#"{"id":1,"result":[]}"#)));
        assert!(unavailable(parse(200, r#"{"id":1,"result":"x"}"#)));
    }

    #[test]
    fn tokens_that_cannot_be_tokens() {
        assert!(plausible_token(
            "eyJ0eXAiOiJKV1Qi.eyJpZCI6InVzZXI6YWJjIn0.c2ln"
        ));
        assert!(!plausible_token(""));
        assert!(!plausible_token("two words"));
        assert!(!plausible_token("line\nbreak"));
        assert!(!plausible_token("tökén"));
        assert!(!plausible_token(&"a".repeat(MAX_TOKEN_BYTES + 1)));
        assert!(plausible_token(&"a".repeat(MAX_TOKEN_BYTES)));
    }

    #[test]
    fn the_request_body_is_the_query() {
        let body: Value = serde_json::from_str(QUERY).unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "id": 1,
                "method": "query",
                "params": ["RETURN [$auth.id, session::ns(), session::db()];", {}],
            })
        );
    }
}
