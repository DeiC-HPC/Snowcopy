use aws_smithy_runtime_api::client::interceptors::context::{BeforeDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextRef};
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use tokio::sync::mpsc::UnboundedSender;
use util_layer::cc_util::CommandCenterEvent;

#[derive(Debug, Clone)]
pub(crate) struct LoggingInterceptor {
    pub(crate) cc_tx: UnboundedSender<CommandCenterEvent>,
}

impl Intercept for LoggingInterceptor {
    fn name(&self) -> &'static str {
        "LoggingInterceptor"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), aws_smithy_runtime_api::box_error::BoxError> {
        let request = context.request();
        let headers_str = request.headers().iter()
            .map(|(name, value)| {
                if is_sensitive(name) {
                    format!("  {}: <redacted>", name)
                } else {
                    format!("  {}: {}", name, value)
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        let log_msg = format!(
            "outgoing request: method={}, uri={}, headers={}",
            request.method(),
            request.uri(),
            format!("\n{}", headers_str)
        );
        let _ = self.cc_tx.send(CommandCenterEvent::Log(log_msg));
        Ok(())
    }

    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), aws_smithy_runtime_api::box_error::BoxError> {
        let response = context.response();
        let headers_str = response.headers().iter()
            .map(|(name, value)| {
                if is_sensitive(name) {
                    format!("  {}: <redacted>", name)
                } else {
                    format!("  {}: {}", name, value)
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        let log_msg = format!(
            "incoming response: status={}, headers={}",
            response.status(),
            format!("\n{}", headers_str)
        );
        let _ = self.cc_tx.send(CommandCenterEvent::Log(log_msg));
        Ok(())
    }
}

fn is_sensitive(name: &str) -> bool { //TODO Jon: http layer has the same functionality. It should be shared in a layer that both aws and http layer have. I don't want to create a new layer now as it will mess up LND's work
    matches!(
        name.to_ascii_lowercase().as_str(),
        "authorization" | "proxy-authorization" | "cookie" | "set-cookie" | "x-api-key"
    )
}