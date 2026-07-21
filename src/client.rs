use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow, bail};
use rdkafka::ClientConfig;
use rdkafka::admin::AdminClient;
use rdkafka::bindings as rdsys;
use rdkafka::client::{Client, ClientContext, DefaultClientContext};
use rdkafka::consumer::{BaseConsumer, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, Producer};

#[derive(Debug, Clone)]
pub struct GlobalOptions {
    pub brokers: String,
    pub timeout_ms: u64,
    pub ssl: bool,
    pub insecure: bool,
    pub mechanism: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub oauth_bearer: Option<String>,
    pub oauth_principal: Option<String>,
    pub oauth_expiry_ms: Option<i64>,
    pub oidc_token_url: Option<String>,
    pub oidc_client_id: Option<String>,
    pub oidc_client_secret: Option<String>,
    pub oidc_scope: Option<String>,
    pub oidc_extensions: Option<String>,
}

struct StaticOAuth<'a> {
    token: &'a str,
    principal: &'a str,
    expiry_ms: i64,
}

impl GlobalOptions {
    pub fn operation_timeout(&self) -> std::time::Duration {
        if self.timeout_ms == 0 {
            std::time::Duration::from_secs(30)
        } else {
            std::time::Duration::from_millis(self.timeout_ms)
        }
    }
}

fn normalize_mechanism(raw: &str) -> Result<&'static str> {
    match raw.to_ascii_lowercase().as_str() {
        "plain" => Ok("PLAIN"),
        "scram-sha-256" => Ok("SCRAM-SHA-256"),
        "scram-sha-512" => Ok("SCRAM-SHA-512"),
        "oauthbearer" => Ok("OAUTHBEARER"),
        other => bail!("Unsupported SASL mechanism: {other}"),
    }
}

fn now_millis() -> Result<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| anyhow!("System clock is before the Unix epoch: {e}"))?
        .as_millis();
    i64::try_from(millis).map_err(|_| anyhow!("Current timestamp does not fit in i64"))
}

fn static_oauth(opts: &GlobalOptions) -> Result<Option<StaticOAuth<'_>>> {
    match (
        opts.oauth_bearer.as_deref(),
        opts.oauth_principal.as_deref(),
        opts.oauth_expiry_ms,
    ) {
        (None, None, None) => Ok(None),
        (Some(token), Some(principal), Some(expiry_ms)) => {
            if token.is_empty() {
                bail!("--oauth-bearer must not be empty");
            }
            if principal.is_empty() {
                bail!("--oauth-principal must not be empty");
            }
            if expiry_ms <= now_millis()? {
                bail!("--oauth-expiry-ms must be a future Unix timestamp in milliseconds");
            }
            Ok(Some(StaticOAuth {
                token,
                principal,
                expiry_ms,
            }))
        }
        _ => bail!(
            "--oauth-bearer, --oauth-principal, and --oauth-expiry-ms must be provided together"
        ),
    }
}

fn oidc_requested(opts: &GlobalOptions) -> bool {
    opts.oidc_token_url.is_some()
        || opts.oidc_client_id.is_some()
        || opts.oidc_client_secret.is_some()
        || opts.oidc_scope.is_some()
        || opts.oidc_extensions.is_some()
}

pub fn build_client_config(opts: &GlobalOptions) -> Result<ClientConfig> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &opts.brokers);
    config.set("client.id", "kafq");

    let static_oauth = static_oauth(opts)?;
    let oidc_requested = oidc_requested(opts);
    if static_oauth.is_some() && oidc_requested {
        bail!("Static OAUTHBEARER and OIDC options are mutually exclusive");
    }

    let mut sasl_protocol = None;
    if let Some(ref mech) = opts.mechanism {
        let mechanism = normalize_mechanism(mech)?;
        config.set("sasl.mechanism", mechanism);
        if mechanism == "OAUTHBEARER" {
            if oidc_requested {
                let token_url = opts.oidc_token_url.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("--oidc-token-url is required when using SASL OAUTHBEARER OIDC")
                })?;
                let client_id = opts.oidc_client_id.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("--oidc-client-id is required when using SASL OAUTHBEARER OIDC")
                })?;
                let client_secret = opts.oidc_client_secret.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "--oidc-client-secret is required when using SASL OAUTHBEARER OIDC"
                    )
                })?;
                config.set("sasl.oauthbearer.method", "oidc");
                config.set("sasl.oauthbearer.token.endpoint.url", token_url);
                config.set("sasl.oauthbearer.client.id", client_id);
                config.set("sasl.oauthbearer.client.secret", client_secret);
                if let Some(ref scope) = opts.oidc_scope {
                    config.set("sasl.oauthbearer.scope", scope);
                }
                if let Some(ref ext) = opts.oidc_extensions {
                    config.set("sasl.oauthbearer.extensions", ext);
                }
            } else if static_oauth.is_none() {
                bail!("SASL OAUTHBEARER requires either static token options or OIDC options");
            }
        } else {
            if static_oauth.is_some() || oidc_requested {
                bail!("OAuth options require --mechanism oauthbearer");
            }
            if let Some(ref u) = opts.username {
                config.set("sasl.username", u);
            }
            if let Some(ref p) = opts.password {
                config.set("sasl.password", p);
            }
        }
        sasl_protocol = Some(if opts.ssl { "SASL_SSL" } else { "SASL_PLAINTEXT" });
    } else if static_oauth.is_some() || oidc_requested {
        bail!("OAuth options require --mechanism oauthbearer");
    }

    let protocol = sasl_protocol.unwrap_or(if opts.ssl { "SSL" } else { "PLAINTEXT" });
    config.set("security.protocol", protocol);

    if opts.ssl && opts.insecure {
        config.set("enable.ssl.certificate.verification", "false");
    }

    Ok(config)
}

fn install_static_oauth<C: ClientContext>(opts: &GlobalOptions, client: &Client<C>) -> Result<()> {
    let Some(oauth) = static_oauth(opts)? else {
        return Ok(());
    };

    let token = CString::new(oauth.token)
        .map_err(|_| anyhow!("--oauth-bearer must not contain a NUL byte"))?;
    let principal = CString::new(oauth.principal)
        .map_err(|_| anyhow!("--oauth-principal must not contain a NUL byte"))?;
    let mut error_buffer = vec![0 as c_char; 512];
    let code = unsafe {
        rdsys::rd_kafka_oauthbearer_set_token(
            client.native_ptr(),
            token.as_ptr(),
            oauth.expiry_ms,
            principal.as_ptr(),
            ptr::null_mut(),
            0,
            error_buffer.as_mut_ptr(),
            error_buffer.len(),
        )
    };
    if code != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        let detail = unsafe { CStr::from_ptr(error_buffer.as_ptr()) }.to_string_lossy();
        bail!("Failed to install static OAUTHBEARER token ({code:?}): {detail}");
    }
    Ok(())
}

pub fn create_admin(
    config: &ClientConfig,
    opts: &GlobalOptions,
) -> Result<AdminClient<DefaultClientContext>> {
    let client: AdminClient<DefaultClientContext> = config.create()?;
    install_static_oauth(opts, client.inner())?;
    Ok(client)
}

pub fn create_base_consumer(config: &ClientConfig, opts: &GlobalOptions) -> Result<BaseConsumer> {
    let client: BaseConsumer = config.create()?;
    install_static_oauth(opts, client.client())?;
    Ok(client)
}

pub fn create_stream_consumer(
    config: &ClientConfig,
    opts: &GlobalOptions,
) -> Result<StreamConsumer> {
    let client: StreamConsumer = config.create()?;
    install_static_oauth(opts, client.client())?;
    Ok(client)
}

fn producer_config(config: &ClientConfig, opts: &GlobalOptions) -> ClientConfig {
    let mut config = config.clone();
    config.set("enable.idempotence", "true");
    config.set("delivery.timeout.ms", opts.timeout_ms.to_string());
    config
}

pub fn create_producer(config: &ClientConfig, opts: &GlobalOptions) -> Result<FutureProducer> {
    let config = producer_config(config, opts);
    let client: FutureProducer = config.create()?;
    install_static_oauth(opts, client.client())?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> GlobalOptions {
        GlobalOptions {
            brokers: "localhost:9092".to_string(),
            timeout_ms: 0,
            ssl: false,
            insecure: false,
            mechanism: None,
            username: None,
            password: None,
            oauth_bearer: None,
            oauth_principal: None,
            oauth_expiry_ms: None,
            oidc_token_url: None,
            oidc_client_id: None,
            oidc_client_secret: None,
            oidc_scope: None,
            oidc_extensions: None,
        }
    }

    #[test]
    fn static_oauth_requires_complete_metadata() {
        let mut opts = options();
        opts.mechanism = Some("oauthbearer".to_string());
        opts.oauth_bearer = Some("token".to_string());

        let error = build_client_config(&opts).unwrap_err();

        assert!(error.to_string().contains("must be provided together"));
    }

    #[test]
    fn static_oauth_rejects_expired_tokens() {
        let mut opts = options();
        opts.mechanism = Some("oauthbearer".to_string());
        opts.oauth_bearer = Some("token".to_string());
        opts.oauth_principal = Some("principal".to_string());
        opts.oauth_expiry_ms = Some(1);

        let error = build_client_config(&opts).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("must be a future Unix timestamp")
        );
    }

    #[test]
    fn static_oauth_uses_the_application_token_api() {
        let mut opts = options();
        opts.mechanism = Some("oauthbearer".to_string());
        opts.oauth_bearer = Some("token".to_string());
        opts.oauth_principal = Some("principal".to_string());
        opts.oauth_expiry_ms = Some(now_millis().unwrap() + 60_000);

        let config = build_client_config(&opts).unwrap();

        assert_eq!(config.get("sasl.mechanism"), Some("OAUTHBEARER"));
        assert_eq!(config.get("sasl.oauthbearer.config"), None);
    }

    #[test]
    fn static_oauth_installs_for_every_client_type() {
        let mut opts = options();
        opts.mechanism = Some("oauthbearer".to_string());
        opts.oauth_bearer = Some("token".to_string());
        opts.oauth_principal = Some("principal".to_string());
        opts.oauth_expiry_ms = Some(now_millis().unwrap() + 60_000);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _runtime_guard = runtime.enter();

        let admin_config = build_client_config(&opts).unwrap();
        let _admin = create_admin(&admin_config, &opts).unwrap();

        let mut base_config = build_client_config(&opts).unwrap();
        base_config.set("group.id", "static-oauth-base-test");
        let _base = create_base_consumer(&base_config, &opts).unwrap();

        let mut stream_config = build_client_config(&opts).unwrap();
        stream_config.set("group.id", "static-oauth-stream-test");
        let _stream = create_stream_consumer(&stream_config, &opts).unwrap();

        let producer_config = build_client_config(&opts).unwrap();
        let _producer = create_producer(&producer_config, &opts).unwrap();
    }

    #[test]
    fn producer_timeout_bounds_delivery() {
        let mut opts = options();
        opts.timeout_ms = 1_234;
        let config = producer_config(&build_client_config(&opts).unwrap(), &opts);

        assert_eq!(config.get("delivery.timeout.ms"), Some("1234"));
    }

    #[test]
    fn zero_producer_timeout_disables_delivery_timeout() {
        let opts = options();
        let config = producer_config(&build_client_config(&opts).unwrap(), &opts);

        assert_eq!(config.get("delivery.timeout.ms"), Some("0"));
    }

    #[test]
    fn producer_enables_idempotence() {
        let opts = options();
        let config = producer_config(&build_client_config(&opts).unwrap(), &opts);

        assert_eq!(config.get("enable.idempotence"), Some("true"));
    }
}
