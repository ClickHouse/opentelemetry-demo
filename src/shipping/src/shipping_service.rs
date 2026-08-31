// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use opentelemetry::trace::{FutureExt, SpanKind, TraceContextExt, Tracer};
use opentelemetry::{global, propagation::Extractor, trace::Span, Context, KeyValue};
use opentelemetry_semantic_conventions as semconv;
use shop::shipping_service_server::ShippingService;
use shop::{GetQuoteRequest, GetQuoteResponse, Money, ShipOrderRequest, ShipOrderResponse};
use tonic::{Request, Response, Status};
use rand::Rng;

use log::*;

mod quote;
use quote::create_quote_from_count;

mod tracking;
use tracking::create_tracking_id;

const NANOS_MULTIPLE: i32 = 10000000i32;

const RPC_SYSTEM_GRPC: &'static str = "grpc";
const RPC_SERVICE_SHIPPING: &'static str = "oteldemo.ShippingService";
const RPC_GRPC_STATUS_CODE_OK: i64 = 0;
const RPC_GRPC_STATUS_CODE_UNKNOWN: i64 = 2;

const CARRIERS: &[&str] = &["fedex", "ups", "usps", "dhl", "amazon-logistics", "ontrac", "lasership", "canada-post", "royal-mail", "dpd"];
const SHIPPING_METHODS: &[&str] = &["ground", "express", "next-day", "two-day", "economy", "freight", "white-glove", "same-day"];
const PACKAGE_TYPES: &[&str] = &["box-small", "box-medium", "box-large", "envelope", "tube", "pallet", "custom", "padded-envelope"];
const WAREHOUSE_ZONES: &[&str] = &["zone-a-east", "zone-b-west", "zone-c-central", "zone-d-south", "zone-e-north"];
const FULFILLMENT_CENTERS: &[&str] = &["FC-EWR-01", "FC-LAX-02", "FC-ORD-03", "FC-DFW-04", "FC-ATL-05", "FC-SEA-06", "FC-MIA-07"];
const SORT_FACILITIES: &[&str] = &["SORT-NYC", "SORT-CHI", "SORT-LAX", "SORT-DAL", "SORT-ATL", "SORT-SEA"];
const LABEL_FORMATS: &[&str] = &["zpl-4x6", "pdf-4x6", "png-4x6", "epl-4x6"];
const CUSTOMS_TYPES: &[&str] = &["domestic", "international-standard", "international-express", "cross-border-economy"];
const INSURANCE_TIERS: &[&str] = &["none", "basic-100", "standard-500", "premium-2000", "full-value"];
const ROUTING_ALGORITHMS: &[&str] = &["shortest-path", "cost-optimized", "speed-optimized", "eco-friendly", "balanced"];
const PACKAGING_MATERIALS: &[&str] = &["corrugated-cardboard", "recycled-kraft", "bubble-wrap", "foam-insert", "air-pillows", "eco-peanuts"];
const HAZMAT_CLASSES: &[&str] = &["none", "none", "none", "none", "class-1-limited", "class-9-misc", "orm-d"];
const DELIVERY_WINDOWS: &[&str] = &["morning-8-12", "afternoon-12-5", "evening-5-9", "all-day", "appointment"];
const RETURN_POLICIES: &[&str] = &["prepaid-label", "customer-pays", "free-return", "no-return", "exchange-only"];

pub mod shop {
    tonic::include_proto!("oteldemo"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct ShippingServer {}

struct MetadataMap<'a>(&'a tonic::metadata::MetadataMap);

impl<'a> Extractor for MetadataMap<'a> {
    /// Get a value for a key from the MetadataMap.  If the value can't be converted to &str, returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

#[tonic::async_trait]
impl ShippingService for ShippingServer {
    async fn get_quote(
        &self,
        request: Request<GetQuoteRequest>,
    ) -> Result<Response<GetQuoteResponse>, Status> {
        debug!("GetQuoteRequest: {:?}", request);
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));

        let request_message = request.into_inner();

        let itemct: u32 = request_message
            .items
            .into_iter()
            .fold(0, |accum, cart_item| accum + (cart_item.quantity as u32));

        // We may want to ask another service for product pricing / info
        // (although now everything is assumed to be the same price)
        // check out the create_quote_from_count method to see how we use the span created here
        let tracer = global::tracer("shipping");
        let mut span = tracer
            .span_builder("oteldemo.ShippingService/GetQuote")
            .with_kind(SpanKind::Server)
            .start_with_context(&tracer, &parent_cx);
        span.set_attribute(KeyValue::new(semconv::trace::RPC_SYSTEM, RPC_SYSTEM_GRPC));
        span.set_attribute(KeyValue::new(semconv::trace::RPC_SERVICE, RPC_SERVICE_SHIPPING));
        span.set_attribute(KeyValue::new(semconv::trace::RPC_METHOD, "GetQuote"));

        span.add_event("Processing get quote request".to_string(), vec![]);
        let address = request_message.address.unwrap();
        let zip_code = address.zip_code.clone();
        span.set_attribute(KeyValue::new("app.shipping.zip_code", zip_code.clone()));

        // Rich shipping quote attributes — scoped so rng is dropped before .await
        {
            let mut rng = rand::thread_rng();
            let carrier = CARRIERS[rng.gen_range(0..CARRIERS.len())];
            let method = SHIPPING_METHODS[rng.gen_range(0..SHIPPING_METHODS.len())];
            span.set_attribute(KeyValue::new("app.shipping.carrier", carrier.to_string()));
            span.set_attribute(KeyValue::new("app.shipping.method", method.to_string()));
            span.set_attribute(KeyValue::new("app.shipping.package_type", PACKAGE_TYPES[rng.gen_range(0..PACKAGE_TYPES.len())].to_string()));
            span.set_attribute(KeyValue::new("app.shipping.items_count", itemct as i64));
            span.set_attribute(KeyValue::new("app.shipping.total_weight_kg", (rng.gen::<f64>() * 20.0 + 0.1) as f64));
            span.set_attribute(KeyValue::new("app.shipping.dimensions_cm", format!("{}x{}x{}", rng.gen_range(10..60), rng.gen_range(10..40), rng.gen_range(5..30))));
            span.set_attribute(KeyValue::new("app.shipping.is_oversized", rng.gen_bool(0.05)));
            span.set_attribute(KeyValue::new("app.shipping.is_fragile", rng.gen_bool(0.15)));
            span.set_attribute(KeyValue::new("app.shipping.requires_signature", rng.gen_bool(0.2)));
            span.set_attribute(KeyValue::new("app.shipping.insurance_tier", INSURANCE_TIERS[rng.gen_range(0..INSURANCE_TIERS.len())].to_string()));
            span.set_attribute(KeyValue::new("app.shipping.hazmat_class", HAZMAT_CLASSES[rng.gen_range(0..HAZMAT_CLASSES.len())].to_string()));
            span.set_attribute(KeyValue::new("app.shipping.customs_type", CUSTOMS_TYPES[rng.gen_range(0..CUSTOMS_TYPES.len())].to_string()));
            span.set_attribute(KeyValue::new("app.shipping.packaging_material", PACKAGING_MATERIALS[rng.gen_range(0..PACKAGING_MATERIALS.len())].to_string()));

            // Origin and destination
            span.set_attribute(KeyValue::new("app.shipping.origin.warehouse", WAREHOUSE_ZONES[rng.gen_range(0..WAREHOUSE_ZONES.len())].to_string()));
            span.set_attribute(KeyValue::new("app.shipping.origin.fulfillment_center", FULFILLMENT_CENTERS[rng.gen_range(0..FULFILLMENT_CENTERS.len())].to_string()));
            span.set_attribute(KeyValue::new("app.shipping.origin.sort_facility", SORT_FACILITIES[rng.gen_range(0..SORT_FACILITIES.len())].to_string()));
            span.set_attribute(KeyValue::new("app.shipping.destination.country", address.country.clone()));
            span.set_attribute(KeyValue::new("app.shipping.destination.state", address.state.clone()));
            span.set_attribute(KeyValue::new("app.shipping.destination.city", address.city.clone()));
            span.set_attribute(KeyValue::new("app.shipping.destination.zip", zip_code.clone()));
            span.set_attribute(KeyValue::new("app.shipping.destination.is_residential", rng.gen_bool(0.7)));
            span.set_attribute(KeyValue::new("app.shipping.destination.is_po_box", rng.gen_bool(0.05)));

            // Routing and estimation
            span.set_attribute(KeyValue::new("app.shipping.routing_algorithm", ROUTING_ALGORITHMS[rng.gen_range(0..ROUTING_ALGORITHMS.len())].to_string()));
            span.set_attribute(KeyValue::new("app.shipping.estimated_days", rng.gen_range(1..15) as i64));
            span.set_attribute(KeyValue::new("app.shipping.distance_km", rng.gen_range(10..5000) as i64));
            span.set_attribute(KeyValue::new("app.shipping.zone", rng.gen_range(1..9) as i64));
            span.set_attribute(KeyValue::new("app.shipping.delivery_window", DELIVERY_WINDOWS[rng.gen_range(0..DELIVERY_WINDOWS.len())].to_string()));
            span.set_attribute(KeyValue::new("app.shipping.return_policy", RETURN_POLICIES[rng.gen_range(0..RETURN_POLICIES.len())].to_string()));
            span.set_attribute(KeyValue::new("app.shipping.label_format", LABEL_FORMATS[rng.gen_range(0..LABEL_FORMATS.len())].to_string()));

            // Cost breakdown
            span.set_attribute(KeyValue::new("app.shipping.base_cost_usd", (rng.gen::<f64>() * 20.0 + 3.0) as f64));
            span.set_attribute(KeyValue::new("app.shipping.fuel_surcharge_usd", (rng.gen::<f64>() * 5.0) as f64));
            span.set_attribute(KeyValue::new("app.shipping.handling_fee_usd", (rng.gen::<f64>() * 3.0) as f64));
            span.set_attribute(KeyValue::new("app.shipping.insurance_cost_usd", (rng.gen::<f64>() * 10.0) as f64));
            span.set_attribute(KeyValue::new("app.shipping.discount_applied", rng.gen_bool(0.25)));
            span.set_attribute(KeyValue::new("app.shipping.discount_pct", rng.gen_range(0..30) as i64));

            // Rate calculation
            span.set_attribute(KeyValue::new("app.shipping.rate_api_call_ms", rng.gen_range(10..300) as i64));
            span.set_attribute(KeyValue::new("app.shipping.rate_cache_hit", rng.gen_bool(0.6)));
            span.set_attribute(KeyValue::new("app.shipping.rates_compared", rng.gen_range(2..8) as i64));
            span.set_attribute(KeyValue::new("app.shipping.cheapest_option", carrier.to_string()));

            // Sustainability
            span.set_attribute(KeyValue::new("app.shipping.carbon_offset_kg", (rng.gen::<f64>() * 5.0) as f64));
            span.set_attribute(KeyValue::new("app.shipping.eco_friendly_packaging", rng.gen_bool(0.4)));

            // Infrastructure
            span.set_attribute(KeyValue::new("app.infra.handler_instance", format!("shipping-{}", rng.gen_range(0..5))));
            span.set_attribute(KeyValue::new("app.infra.memory_usage_mb", rng.gen_range(32..256) as i64));
            span.set_attribute(KeyValue::new("app.infra.active_requests", rng.gen_range(1..50) as i64));

            // Log with rich context
            let quote_log_carrier = CARRIERS[rng.gen_range(0..CARRIERS.len())];
            let quote_log_warehouse = FULFILLMENT_CENTERS[rng.gen_range(0..FULFILLMENT_CENTERS.len())];
            info!("Sending Quote | {{\"shipping.operation\":\"get_quote\",\"shipping.carrier\":\"{}\",\"shipping.method\":\"{}\",\"shipping.items_count\":{},\"shipping.weight_kg\":{:.2},\"shipping.zip_code\":\"{}\",\"shipping.destination_country\":\"{}\",\"shipping.estimated_days\":{},\"shipping.zone\":{},\"shipping.distance_km\":{},\"shipping.base_cost_usd\":{:.2},\"shipping.fuel_surcharge_usd\":{:.2},\"shipping.insurance_tier\":\"{}\",\"shipping.routing_algorithm\":\"{}\",\"shipping.fulfillment_center\":\"{}\",\"shipping.rate_cache_hit\":{},\"shipping.rate_api_ms\":{},\"shipping.rates_compared\":{},\"shipping.eco_packaging\":{},\"shipping.carbon_offset_kg\":{:.2},\"infra.handler_instance\":\"shipping-{}\",\"infra.memory_mb\":{},\"infra.active_requests\":{},\"net.peer_address\":\"10.0.{}.{}\"}}",
                quote_log_carrier, method,
                itemct, rng.gen::<f64>() * 20.0 + 0.1, zip_code,
                address.country, rng.gen_range(1..15), rng.gen_range(1..9),
                rng.gen_range(10..5000), rng.gen::<f64>() * 20.0 + 3.0,
                rng.gen::<f64>() * 5.0,
                INSURANCE_TIERS[rng.gen_range(0..INSURANCE_TIERS.len())],
                ROUTING_ALGORITHMS[rng.gen_range(0..ROUTING_ALGORITHMS.len())],
                quote_log_warehouse, rng.gen_bool(0.6), rng.gen_range(10..300),
                rng.gen_range(2..8), rng.gen_bool(0.4), rng.gen::<f64>() * 5.0,
                rng.gen_range(0..5), rng.gen_range(32..256), rng.gen_range(1..50),
                rng.gen_range(0..256), rng.gen_range(1..255)
            );
        } // rng dropped here, before .await

        let cx = Context::current_with_span(span);
        let q = match create_quote_from_count(itemct)
            .with_context(cx.clone())
            .await
        {
            Ok(quote) => quote,
            Err(status) => {
                cx.span().set_attribute(KeyValue::new(
                    semconv::trace::RPC_GRPC_STATUS_CODE,
                    RPC_GRPC_STATUS_CODE_UNKNOWN,
                ));
                return Err(status);
            }
        };

        let reply = GetQuoteResponse {
            cost_usd: Some(Money {
                currency_code: "USD".into(),
                units: q.dollars,
                nanos: q.cents * NANOS_MULTIPLE,
            }),
        };
        info!("Sending Quote: {}", q);

        cx.span().set_attribute(KeyValue::new(
            semconv::trace::RPC_GRPC_STATUS_CODE,
            RPC_GRPC_STATUS_CODE_OK,
        ));
        Ok(Response::new(reply))
    }
    async fn ship_order(
        &self,
        request: Request<ShipOrderRequest>,
    ) -> Result<Response<ShipOrderResponse>, Status> {
        debug!("ShipOrderRequest: {:?}", request);

        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        // in this case, generating a tracking ID is trivial
        // we'll create a span and associated events all in this function.
        let tracer = global::tracer("shipping");
        let mut span = tracer
            .span_builder("oteldemo.ShippingService/ShipOrder")
            .with_kind(SpanKind::Server)
            .start_with_context(&tracer, &parent_cx);
        span.set_attribute(KeyValue::new(semconv::trace::RPC_SYSTEM, RPC_SYSTEM_GRPC));
        span.set_attribute(KeyValue::new(semconv::trace::RPC_SERVICE, RPC_SERVICE_SHIPPING));
        span.set_attribute(KeyValue::new(semconv::trace::RPC_METHOD, "ShipOrder"));

        span.add_event("Processing shipping order request".to_string(), vec![]);

        let tid = create_tracking_id();
        span.set_attribute(KeyValue::new("app.shipping.tracking.id", tid.clone()));

        // Rich ship order attributes
        let mut rng = rand::thread_rng();
        let carrier = CARRIERS[rng.gen_range(0..CARRIERS.len())];
        let method = SHIPPING_METHODS[rng.gen_range(0..SHIPPING_METHODS.len())];

        let ship_log_fc = FULFILLMENT_CENTERS[rng.gen_range(0..FULFILLMENT_CENTERS.len())];
        info!("Tracking ID Created: {} | {{\"shipment.operation\":\"ship_order\",\"shipment.request_id\":\"{}\",\"shipment.tracking_id\":\"{}\",\"shipment.carrier\":\"{}\",\"shipment.method\":\"{}\",\"shipment.status\":\"label-created\",\"shipment.package_type\":\"{}\",\"shipment.items_count\":{},\"shipment.weight_kg\":{:.2},\"shipment.declared_value_usd\":{:.2},\"shipment.insurance_tier\":\"{}\",\"shipment.requires_signature\":{},\"shipment.estimated_delivery_days\":{},\"shipment.delivery_window\":\"{}\",\"fulfillment.center\":\"{}\",\"fulfillment.warehouse_zone\":\"{}\",\"fulfillment.pick_time_sec\":{},\"fulfillment.pack_time_sec\":{},\"fulfillment.queue_position\":{},\"fulfillment.batch_id\":\"batch-{}\",\"carrier.api_call_ms\":{},\"carrier.api_retries\":{},\"carrier.pickup_scheduled\":{},\"shipment.carbon_offset_kg\":{:.2},\"shipment.eco_packaging\":{},\"infra.handler_instance\":\"shipping-{}\",\"infra.memory_mb\":{},\"infra.active_requests\":{},\"net.peer_address\":\"10.0.{}.{}\"}}",
            tid,
            uuid::Uuid::new_v4(), tid, carrier, method,
            PACKAGE_TYPES[rng.gen_range(0..PACKAGE_TYPES.len())],
            rng.gen_range(1..10), rng.gen::<f64>() * 20.0 + 0.1,
            rng.gen::<f64>() * 1000.0 + 5.0,
            INSURANCE_TIERS[rng.gen_range(0..INSURANCE_TIERS.len())],
            rng.gen_bool(0.2), rng.gen_range(1..14),
            DELIVERY_WINDOWS[rng.gen_range(0..DELIVERY_WINDOWS.len())],
            ship_log_fc,
            WAREHOUSE_ZONES[rng.gen_range(0..WAREHOUSE_ZONES.len())],
            rng.gen_range(30..600), rng.gen_range(20..300), rng.gen_range(1..500),
            rng.gen_range(1000..9999), rng.gen_range(100..2000),
            rng.gen_range(0..3), rng.gen_bool(0.8),
            rng.gen::<f64>() * 5.0, rng.gen_bool(0.4),
            rng.gen_range(0..5), rng.gen_range(32..256), rng.gen_range(1..50),
            rng.gen_range(0..256), rng.gen_range(1..255)
        );

        // Shipment details
        span.set_attribute(KeyValue::new("app.shipment.carrier", carrier.to_string()));
        span.set_attribute(KeyValue::new("app.shipment.method", method.to_string()));
        span.set_attribute(KeyValue::new("app.shipment.status", "label-created".to_string()));
        span.set_attribute(KeyValue::new("app.shipment.package_type", PACKAGE_TYPES[rng.gen_range(0..PACKAGE_TYPES.len())].to_string()));
        span.set_attribute(KeyValue::new("app.shipment.label_format", LABEL_FORMATS[rng.gen_range(0..LABEL_FORMATS.len())].to_string()));
        span.set_attribute(KeyValue::new("app.shipment.label_generated_ms", rng.gen_range(50..500) as i64));
        span.set_attribute(KeyValue::new("app.shipment.items_count", rng.gen_range(1..10) as i64));
        span.set_attribute(KeyValue::new("app.shipment.total_weight_kg", (rng.gen::<f64>() * 20.0 + 0.1) as f64));
        span.set_attribute(KeyValue::new("app.shipment.declared_value_usd", (rng.gen::<f64>() * 1000.0 + 5.0) as f64));
        span.set_attribute(KeyValue::new("app.shipment.insurance_tier", INSURANCE_TIERS[rng.gen_range(0..INSURANCE_TIERS.len())].to_string()));
        span.set_attribute(KeyValue::new("app.shipment.requires_signature", rng.gen_bool(0.2)));
        span.set_attribute(KeyValue::new("app.shipment.customs_type", CUSTOMS_TYPES[rng.gen_range(0..CUSTOMS_TYPES.len())].to_string()));
        span.set_attribute(KeyValue::new("app.shipment.is_priority", rng.gen_bool(0.15)));
        span.set_attribute(KeyValue::new("app.shipment.estimated_delivery_days", rng.gen_range(1..14) as i64));
        span.set_attribute(KeyValue::new("app.shipment.delivery_window", DELIVERY_WINDOWS[rng.gen_range(0..DELIVERY_WINDOWS.len())].to_string()));

        // Fulfillment details
        span.set_attribute(KeyValue::new("app.fulfillment.center", FULFILLMENT_CENTERS[rng.gen_range(0..FULFILLMENT_CENTERS.len())].to_string()));
        span.set_attribute(KeyValue::new("app.fulfillment.warehouse_zone", WAREHOUSE_ZONES[rng.gen_range(0..WAREHOUSE_ZONES.len())].to_string()));
        span.set_attribute(KeyValue::new("app.fulfillment.sort_facility", SORT_FACILITIES[rng.gen_range(0..SORT_FACILITIES.len())].to_string()));
        span.set_attribute(KeyValue::new("app.fulfillment.pick_time_sec", rng.gen_range(30..600) as i64));
        span.set_attribute(KeyValue::new("app.fulfillment.pack_time_sec", rng.gen_range(20..300) as i64));
        span.set_attribute(KeyValue::new("app.fulfillment.queue_position", rng.gen_range(1..500) as i64));
        span.set_attribute(KeyValue::new("app.fulfillment.batch_id", format!("batch-{}", rng.gen_range(1000..9999))));
        span.set_attribute(KeyValue::new("app.fulfillment.packaging_material", PACKAGING_MATERIALS[rng.gen_range(0..PACKAGING_MATERIALS.len())].to_string()));

        // Carrier API interaction
        span.set_attribute(KeyValue::new("app.carrier.api_call_ms", rng.gen_range(100..2000) as i64));
        span.set_attribute(KeyValue::new("app.carrier.api_retries", rng.gen_range(0..3) as i64));
        span.set_attribute(KeyValue::new("app.carrier.rate_limit_remaining", rng.gen_range(100..10000) as i64));
        span.set_attribute(KeyValue::new("app.carrier.pickup_scheduled", rng.gen_bool(0.8)));
        span.set_attribute(KeyValue::new("app.carrier.service_level", format!("{}-{}", carrier, method)));

        // Sustainability
        span.set_attribute(KeyValue::new("app.shipment.carbon_offset_kg", (rng.gen::<f64>() * 5.0) as f64));
        span.set_attribute(KeyValue::new("app.shipment.eco_packaging", rng.gen_bool(0.4)));
        span.set_attribute(KeyValue::new("app.shipment.consolidated", rng.gen_bool(0.1)));

        span.add_event(
            "Shipping tracking id created, response sent back".to_string(),
            vec![],
        );

        span.set_attribute(KeyValue::new(
            semconv::trace::RPC_GRPC_STATUS_CODE,
            RPC_GRPC_STATUS_CODE_OK,
        ));
        Ok(Response::new(ShipOrderResponse { tracking_id: tid }))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        shop::shipping_service_server::ShippingService, shop::ShipOrderRequest, ShippingServer,
    };
    use tonic::Request;
    use uuid::Uuid;

    #[tokio::test]
    async fn can_get_tracking_id() {
        let server = ShippingServer::default();

        match server
            .ship_order(Request::new(ShipOrderRequest::default()))
            .await
        {
            Ok(resp) => {
                // we should see a uuid
                match Uuid::parse_str(&resp.into_inner().tracking_id) {
                    Ok(_) => {}
                    Err(e) => panic!("error when parsing uuid: {}", e),
                }
            }
            Err(e) => panic!("error when making request for tracking ID: {}", e),
        }
    }
}
