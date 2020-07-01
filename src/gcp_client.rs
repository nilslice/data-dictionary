use crate::error::Error;

use gouth::Token;
use reqwest::{
    header::{HeaderMap, AUTHORIZATION},
    IntoUrl, Method, RequestBuilder,
};

pub struct GcpClient {
    client: reqwest::Client,
    token: Token,
}

impl GcpClient {
    pub fn request(&self, method: Method, url: impl IntoUrl) -> Result<RequestBuilder, Error> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            self.get_gcp_auth_token()?
                .parse()
                .expect("failed to parse header value for auth token"),
        );

        Ok(self.client.request(method, url).headers(headers))
    }

    fn get_gcp_auth_token(&self) -> Result<String, Error> {
        match self.token.header_value() {
            Ok(arc) => Ok(arc.as_ref().into()),
            Err(e) => Err(Error::Generic(Box::new(e))),
        }
    }
}

impl Default for GcpClient {
    fn default() -> Self {
        GcpClient {
            client: reqwest::Client::new(),
            token: Token::new().expect("failed to get token for GCP auth"),
        }
    }
}
