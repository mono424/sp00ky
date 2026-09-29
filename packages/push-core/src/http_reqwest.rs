//! [`PushHttp`] over reqwest, for the native hosts (scheduler, standalone SSP).
//!
//! A subscription's endpoint is chosen by a record user, and the engine POSTs
//! to it from inside the platform network. [`crate::endpoint_allowed`] refuses
//! IP literals and local names before a send; this client closes the rest of
//! that hole at connect time: its resolver drops every private, loopback,
//! link-local, CGNAT and otherwise non-global address, so a public name that
//! resolves (or re-resolves) to an internal address cannot be reached either.
//! Redirects are not followed for the same reason.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

use crate::PushHttp;

pub struct ReqwestPushHttp {
    client: reqwest::Client,
}

impl ReqwestPushHttp {
    /// `allow_private`: skip the address filter (local development against a
    /// mock push service). Mirrors `EngineOptions::allow_private_endpoints`.
    pub fn new(allow_private: bool) -> Result<Self, String> {
        // rustls, so ALPN offers h2: APNs refuses HTTP/1.1, and FCM and the
        // browser push services take either.
        let mut builder = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("sp00ky-push/", env!("CARGO_PKG_VERSION")));
        if !allow_private {
            builder = builder.dns_resolver(Arc::new(PublicOnlyResolver));
        }
        let client = builder.build().map_err(|e| e.to_string())?;
        Ok(ReqwestPushHttp { client })
    }
}

#[async_trait::async_trait]
impl PushHttp for ReqwestPushHttp {
    async fn post(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<(u16, String), String> {
        let mut req = self.client.post(url).body(body);
        for (name, value) in headers {
            req = req.header(name, value);
        }
        let resp = req.send().await.map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        // The push services answer with a short JSON or text reason, the
        // OAuth token endpoint with a small JSON; never read an unbounded body.
        let text = match resp.bytes().await {
            Ok(b) => String::from_utf8_lossy(&b[..b.len().min(8192)]).into_owned(),
            Err(_) => String::new(),
        };
        Ok((status, text))
    }
}

/// Resolves like the system resolver, minus every address that is not a
/// public unicast address.
struct PublicOnlyResolver;

impl Resolve for PublicOnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let host = name.as_str().to_string();
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await?
                .filter(|a| is_public(a.ip()))
                .collect();
            if addrs.is_empty() {
                return Err(format!("`{host}` does not resolve to a public address").into());
            }
            let iter: Addrs = Box::new(addrs.into_iter());
            Ok(iter)
        })
    }
}

/// Public unicast only. Written out rather than using the unstable
/// `IpAddr::is_global`.
pub fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_unspecified()
                || v4.is_multicast()
                // 0.0.0.0/8, 100.64.0.0/10 (CGNAT), 192.0.0.0/24, 198.18.0.0/15, 240.0.0.0/4
                || o[0] == 0
                || (o[0] == 100 && (o[1] & 0xC0) == 64)
                || (o[0] == 192 && o[1] == 0 && o[2] == 0)
                || (o[0] == 198 && (o[1] & 0xFE) == 18)
                || o[0] >= 240)
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_public(IpAddr::V4(v4));
            }
            let s = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // fc00::/7 unique local, fe80::/10 link local, 2001:db8::/32 documentation
                || (s[0] & 0xFE00) == 0xFC00
                || (s[0] & 0xFFC0) == 0xFE80
                || (s[0] == 0x2001 && s[1] == 0x0DB8))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_filter() {
        for ip in ["10.1.2.3", "127.0.0.1", "169.254.1.1", "172.16.0.1", "192.168.1.1", "100.64.0.1", "0.0.0.0", "::1", "fd00::1", "fe80::1", "::ffff:10.0.0.1"] {
            assert!(!is_public(ip.parse().unwrap()), "{ip} must be refused");
        }
        for ip in ["142.250.1.1", "2a00:1450:4001::1", "::ffff:8.8.8.8"] {
            assert!(is_public(ip.parse().unwrap()), "{ip} must pass");
        }
    }
}
