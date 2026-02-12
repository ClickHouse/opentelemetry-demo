// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use core::fmt;
use std::{collections::HashMap, env};

use log::info;

#[derive(Debug, Default)]
pub struct Quote {
    pub dollars: i64,
    pub cents: i32,
}

// TODO: Check product catalog for price on each item (will likely need item ID)
pub async fn create_quote_from_count(count: u32) -> Result<Quote, tonic::Status> {
    let f = match request_quote(count).await {
        Ok(float) => float,
        Err(err) => {
            let msg = format!("{}", err);
            return Err(tonic::Status::unknown(msg));
        }
    };

    let q = create_quote_from_float(f);
    Ok(q)
}

async fn request_quote(count: u32) -> Result<f64, Box<dyn std::error::Error>> {
    let quote_service_addr: String = format!(
        "{}{}",
        env::var("QUOTE_ADDR")
            .unwrap_or_else(|_| "http://quote:8080".to_string())
            .parse::<String>()
            .expect("Invalid quote service address"),
        "/getquote"
    );

    let mut reqbody = HashMap::new();
    reqbody.insert("numberOfItems", count);

    let client = reqwest::Client::new();

    let resp = client
        .post(quote_service_addr)
        .json(&reqbody)
        .send()
        .await?
        .text_with_charset("utf-8")
        .await?;

    info!("Received quote: {:?}", resp);

    match resp.parse::<f64>() {
        Ok(f) => Ok(f),
        Err(error) => Err(Box::new(error)),
    }
}

pub fn create_quote_from_float(value: f64) -> Quote {
    Quote {
        dollars: value.floor() as i64,
        cents: ((value * 100_f64) as i32) % 100,
    }
}

impl fmt::Display for Quote {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.dollars, self.cents)
    }
}
