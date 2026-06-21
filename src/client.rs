use anyhow::{Result, bail};
use rdkafka::ClientConfig;

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
    pub oidc_token_url: Option<String>,
    pub oidc_client_id: Option<String>,
    pub oidc_client_secret: Option<String>,
    pub oidc_scope: Option<String>,
    pub oidc_extensions: Option<String>,
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

pub fn build_client_config(opts: &GlobalOptions) -> Result<ClientConfig> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &opts.brokers);
    config.set("client.id", "kafq");

    let mut sasl_protocol = None;
    if let Some(ref mech) = opts.mechanism {
        let mechanism = normalize_mechanism(mech)?;
        config.set("sasl.mechanism", mechanism);
        if mechanism == "OAUTHBEARER" {
            if opts.oidc_token_url.is_some()
                || opts.oidc_client_id.is_some()
                || opts.oidc_client_secret.is_some()
            {
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
            } else {
                let token = opts.oauth_bearer.as_deref().unwrap_or_default();
                config.set("sasl.oauthbearer.config", format!("token={token}"));
            }
        } else {
            if let Some(ref u) = opts.username {
                config.set("sasl.username", u);
            }
            if let Some(ref p) = opts.password {
                config.set("sasl.password", p);
            }
        }
        sasl_protocol = Some(if opts.ssl { "SASL_SSL" } else { "SASL_PLAINTEXT" });
    }

    let protocol = sasl_protocol.unwrap_or(if opts.ssl { "SSL" } else { "PLAINTEXT" });
    config.set("security.protocol", protocol);

    if opts.ssl && opts.insecure {
        config.set("enable.ssl.certificate.verification", "false");
    }

    Ok(config)
}
