use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{self, Poll};

use backoff::{backoff::Backoff, exponential::ExponentialBackoff, SystemClock};
use hyper::client::connect::dns as hyper_dns;
use hyper::service::Service;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    lookup_ip::LookupIpIntoIter,
    system_conf, AsyncResolver, TokioConnection, TokioConnectionProvider, TokioHandle,
};

struct ResolverInner {
    resolver: AsyncResolver<TokioConnection, TokioConnectionProvider>,
    backoff: ExponentialBackoff<SystemClock>,
}

type SharedResolver = Arc<Mutex<ResolverInner>>;

static SYSTEM_CONF: Lazy<std::sync::Mutex<io::Result<(ResolverConfig, ResolverOpts)>>> =
    Lazy::new(|| std::sync::Mutex::new(system_conf::read_system_conf().map_err(io::Error::from)));

#[derive(Clone)]
pub(crate) struct TrustDnsResolver {
    state: Arc<Mutex<State>>,
}

pub(crate) struct SocketAddrs {
    iter: LookupIpIntoIter,
}

#[derive(Clone)]
enum State {
    Init(Option<ExponentialBackoff<SystemClock>>),
    Ready(SharedResolver),
}

impl TrustDnsResolver {
    pub(crate) fn new() -> io::Result<Self> {
        SYSTEM_CONF
            .lock()
            .expect("Failed to lock SYSTEM_CONF")
            .as_ref()
            .map_err(|e| {
                io::Error::new(e.kind(), format!("error reading DNS system conf: {}", e))
            })?;

        // At this stage, we might not have been called in the context of a
        // Tokio Runtime, so we must delay the actual construction of the
        // resolver.
        Ok(TrustDnsResolver {
            state: Arc::new(Mutex::new(State::Init(Some(ExponentialBackoff::default())))),
        })
    }
}

impl Service<hyper_dns::Name> for TrustDnsResolver {
    type Response = SocketAddrs;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: hyper_dns::Name) -> Self::Future {
        let resolver = self.clone();
        Box::pin(async move {
            let mut lock = resolver.state.lock().await;

            let resolver = match &mut *lock {
                State::Init(backoff) => {
                    let resolver = Arc::new(Mutex::new(ResolverInner {
                        resolver: new_resolver().await?,
                        backoff: backoff.take().expect("attempting to reinitialise resolver"),
                    }));
                    *lock = State::Ready(resolver.clone());
                    resolver
                }
                State::Ready(resolver) => resolver.clone(),
            };

            // Don't keep lock once the resolver is constructed, otherwise
            // only one lookup could be done at a time.
            drop(lock);

            let lookup = loop {
                let mut resolver = resolver.lock().await;
                match resolver.resolver.lookup_ip(name.as_str()).await {
                    Ok(lookup) => {
                        resolver.backoff.reset();
                        break lookup;
                    }
                    Err(e) => {
                        let new_system_config =
                            system_conf::read_system_conf().map_err(io::Error::from);
                        if new_system_config.is_ok() {
                            let mut system_config =
                                SYSTEM_CONF.lock().expect("Failed to lock SYSTEM_CONF");
                            match (new_system_config, system_config.as_mut()) {
                                (Ok(ref mut new_system_config), Ok(system_config))
                                    if new_system_config != system_config =>
                                {
                                    std::mem::swap(system_config, new_system_config);
                                    let (config, opts) = system_config.clone();
                                    resolver.resolver =
                                        AsyncResolver::new(config, opts, TokioHandle)?;
                                }
                                _ => (),
                            }
                        };

                        if let Some(delay) = resolver.backoff.next_backoff() {
                            drop(resolver);
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                        return Err(e)?;
                    }
                }
            };
            Ok(SocketAddrs {
                iter: lookup.into_iter(),
            })
        })
    }
}

impl Iterator for SocketAddrs {
    type Item = SocketAddr;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|ip_addr| SocketAddr::new(ip_addr, 0))
    }
}

async fn new_resolver() -> Result<
    AsyncResolver<TokioConnection, TokioConnectionProvider>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let (config, opts) = SYSTEM_CONF
        .lock()
        .expect("Failed to lock SYSTEM_CONF")
        .as_ref()
        .expect("can't construct TrustDnsResolver if SYSTEM_CONF is error")
        .clone();
    let resolver = AsyncResolver::new(config, opts, TokioHandle)?;
    Ok(resolver)
}
