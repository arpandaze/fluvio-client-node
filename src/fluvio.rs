use crate::CLIENT_NOT_FOUND_ERROR_MSG;
use crate::admin::FluvioAdminJS;
use crate::consumer::PartitionConsumerJS;
use crate::producer::TopicProducerJS;
use crate::error::FluvioErrorJS;

use fluvio::TopicProducerConfig;
use fluvio::Compression;
use fluvio::TopicProducerConfigBuilder;
use tracing::debug;

use fluvio::Fluvio;

use node_bindgen::derive::node_bindgen;
use node_bindgen::core::TryIntoJs;
use node_bindgen::core::NjError;
use node_bindgen::core::val::JsEnv;
use node_bindgen::sys::napi_value;
use node_bindgen::core::JSClass;
use node_bindgen::core::val::JsObject;

use crate::{optional_property, must_property};

impl From<Fluvio> for FluvioJS {
    fn from(inner: Fluvio) -> Self {
        Self { inner: Some(inner) }
    }
}

impl TryIntoJs for FluvioJS {
    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value, NjError> {
        debug!("converting FluvioJS to js");
        let new_instance = FluvioJS::new_instance(js_env, vec![])?;
        debug!("instance created");
        if let Some(inner) = self.inner {
            FluvioJS::unwrap_mut(js_env, new_instance)?.set_client(inner);
        }
        Ok(new_instance)
    }
}

pub struct FluvioJS {
    inner: Option<Fluvio>,
}

#[node_bindgen]
impl FluvioJS {
    #[node_bindgen(constructor)]
    pub fn new() -> Self {
        Self { inner: None }
    }

    pub fn set_client(&mut self, client: Fluvio) {
        self.inner.replace(client);
    }

    #[node_bindgen]
    async fn admin(&mut self) -> Result<FluvioAdminJS, FluvioErrorJS> {
        if let Some(client) = &mut self.inner {
            let admin_client = client.admin().await;
            Ok(FluvioAdminJS::from(admin_client))
        } else {
            Err(FluvioErrorJS::new(CLIENT_NOT_FOUND_ERROR_MSG.to_owned()))
        }
    }

    #[node_bindgen]
    async fn partition_consumer(
        &mut self,
        topic: String,
        partition: u32,
    ) -> Result<PartitionConsumerJS, FluvioErrorJS> {
        if let Some(client) = &mut self.inner {
            #[allow(deprecated)]
            Ok(PartitionConsumerJS::from(
                client.partition_consumer(topic, partition).await?,
            ))
        } else {
            Err(FluvioErrorJS::new(CLIENT_NOT_FOUND_ERROR_MSG.to_owned()))
        }
    }

    #[node_bindgen]
    async fn topic_producer(&mut self, topic: String) -> Result<TopicProducerJS, FluvioErrorJS> {
        if let Some(client) = &mut self.inner {
            Ok(TopicProducerJS::from(client.topic_producer(topic).await?))
        } else {
            Err(FluvioErrorJS::new(CLIENT_NOT_FOUND_ERROR_MSG.to_owned()))
        }
    }

    #[node_bindgen]
    async fn topic_producer_with_config(
        &mut self,
        topic: String,
        config_obj: JsObject,
    ) -> Result<TopicProducerJS, FluvioErrorJS> {
        if let Some(client) = &mut self.inner {
            // Start with a default config
            let mut config = TopicProducerConfigBuilder::default();

            // Handle properties manually without using the macros directly
            if let Some(prop) = config_obj.get_property("batchSize").map_err(|e| {
                FluvioErrorJS::new(format!("Error getting batchSize property: {}", e))
            })? {
                if let Ok(batch_size) = prop.as_value::<u32>() {
                    // Use the with_* methods which return a new instance
                    config = config.batch_size(batch_size as usize);
                }
            }

            if let Some(prop) = config_obj.get_property("timeoutMs").map_err(|e| {
                FluvioErrorJS::new(format!("Error getting timeoutMs property: {}", e))
            })? {
                if let Ok(timeout_ms) = prop.as_value::<f64>() {
                    config = config.timeout(std::time::Duration::from_millis(timeout_ms as u64));
                }
            }

            if let Some(prop) = config_obj.get_property("lingerMs").map_err(|e| {
                FluvioErrorJS::new(format!("Error getting lingerMs property: {}", e))
            })? {
                if let Ok(linger_ms) = prop.as_value::<f64>() {
                    config = config.linger(std::time::Duration::from_millis(linger_ms as u64));
                }
            }

            if let Some(prop) = config_obj.get_property("maxRequestSize").map_err(|e| {
                FluvioErrorJS::new(format!("Error getting maxRequestSize property: {}", e))
            })? {
                if let Ok(max_request_size) = prop.as_value::<u32>() {
                    config = config.max_request_size(max_request_size as usize);
                }
            }

            if let Some(prop) = config_obj.get_property("compression").map_err(|e| {
                FluvioErrorJS::new(format!("Error getting compression property: {}", e))
            })? {
                if let Ok(compression_type) = prop.as_value::<String>() {
                    let compression = match compression_type.as_str() {
                        "none" => None,
                        "gzip" => Some(Compression::Gzip),
                        "snappy" => Some(Compression::Snappy),
                        "lz4" => Some(Compression::Lz4),
                        _ => {
                            return Err(FluvioErrorJS::new(format!(
                                "Invalid compression type: {}",
                                compression_type
                            )))
                        }
                    };

                    if let Some(compression) = compression {
                        config = config.compression(compression);
                    }
                }
            }

            Ok(TopicProducerJS::from(
                client
                    .topic_producer_with_config(
                        topic,
                        config.build().expect("Failed to build config"),
                    )
                    .await?,
            ))
        } else {
            Err(FluvioErrorJS::new(CLIENT_NOT_FOUND_ERROR_MSG.to_owned()))
        }
    }
}
