use serde::Deserialize;
#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    #[serde(alias = "injectIncludeUsage")]
    pub inject_include_usage: Option<bool>,
    #[serde(alias = "metricName")]
    pub metric_name: Option<String>,
    #[serde(
        alias = "notificationUrl",
        default,
        deserialize_with = "pdk::serde::deserialize_service_opt"
    )]
    pub notification_url: Option<pdk::hl::Service>,
}
#[pdk::hl::entrypoint_flex]
fn init(abi: &dyn pdk::flex_abi::api::FlexAbi) -> Result<(), anyhow::Error> {
    let config: Config = serde_json::from_slice(abi.get_configuration())
        .map_err(|err| {
            anyhow::anyhow!(
                "Failed to parse configuration '{}'. Cause: {}",
                String::from_utf8_lossy(abi.get_configuration()), err
            )
        })?;
    if config.notification_url.is_some() {
        let service = config.notification_url.unwrap();
        abi.service_create(service)?;
    }
    abi.setup()?;
    Ok(())
}
