#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


import json
import os
import random
import uuid
import logging
import re
import hashlib
import time
import string

from locust import HttpUser, task, between
from locust_plugins.users.playwright import PlaywrightUser, pw, PageWithRetry, event

from opentelemetry import context, baggage, trace
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.jinja2 import Jinja2Instrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.instrumentation.urllib3 import URLLib3Instrumentor
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
    OTLPLogExporter,
)
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from openfeature import api
from openfeature.contrib.provider.ofrep import OFREPProvider
from openfeature.contrib.hook.opentelemetry import TracingHook

from playwright.async_api import Route, Request

logger_provider = LoggerProvider(resource=Resource.create(
        {
            "service.name": "load-generator",
        }
    ),)
set_logger_provider(logger_provider)

exporter = OTLPLogExporter(insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)

# Attach OTLP handler to locust logger
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

exporter = OTLPMetricExporter(insecure=True)
set_meter_provider(MeterProvider([PeriodicExportingMetricReader(exporter)]))

tracer_provider = TracerProvider()
trace.set_tracer_provider(tracer_provider)
tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))

# Instrumenting manually to avoid error with locust gevent monkey
Jinja2Instrumentor().instrument()
RequestsInstrumentor().instrument()
SystemMetricsInstrumentor().instrument()
URLLib3Instrumentor().instrument()
logging.info("Instrumentation complete")

# Initialize Flagd provider
base_url = f"http://{os.environ.get('FLAGD_HOST', 'localhost')}:{os.environ.get('FLAGD_OFREP_PORT', 8016)}"
api.set_provider(OFREPProvider(base_url=base_url))
api.add_hooks([TracingHook()])

def get_flagd_value(FlagName):
    # Initialize OpenFeature
    client = api.get_client()
    return client.get_integer_value(FlagName, 0)

# Realistic user agent strings
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Safari/604.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]

LOCALES = ["en-US", "en-GB", "de-DE", "fr-FR", "ja-JP", "es-ES", "pt-BR", "zh-CN", "ko-KR", "it-IT", "nl-NL", "sv-SE", "pl-PL", "ru-RU"]
COUNTRIES = ["US", "GB", "DE", "FR", "JP", "ES", "BR", "CN", "KR", "IT", "NL", "SE", "PL", "RU", "CA", "AU", "IN", "MX"]
TIMEZONES = ["America/New_York", "America/Los_Angeles", "America/Chicago", "Europe/London", "Europe/Berlin", "Europe/Paris", "Asia/Tokyo", "Asia/Shanghai", "Asia/Seoul", "America/Sao_Paulo"]
PLATFORMS = ["web", "mobile-ios", "mobile-android", "tablet-ios", "tablet-android", "desktop-app"]
REFERRERS = ["google.com", "bing.com", "facebook.com", "twitter.com", "instagram.com", "tiktok.com", "youtube.com", "reddit.com", "direct", "email-campaign", "affiliate-partner-1", "affiliate-partner-2", "newsletter"]
CAMPAIGN_IDS = ["spring-sale-2024", "black-friday", "cyber-monday", "summer-clearance", "new-arrivals", "loyalty-reward", "retargeting-v2", "brand-awareness-q1", "holiday-special", "flash-sale-48h"]
AB_TESTS = ["checkout-flow-v3", "recommendation-algo-v2", "pricing-display-test", "search-ranking-exp", "cart-upsell-v1", "homepage-hero-test", "pdp-layout-v2", "nav-redesign-a", "filter-ux-test", "shipping-options-v3"]
USER_TIERS = ["free", "basic", "premium", "enterprise", "vip"]
DEVICE_TYPES = ["desktop", "mobile", "tablet", "smart-tv", "wearable"]
SCREEN_RESOLUTIONS = ["1920x1080", "2560x1440", "3840x2160", "1366x768", "1536x864", "1440x900", "390x844", "412x915", "360x780", "768x1024"]
NETWORK_TYPES = ["wifi", "4g", "5g", "3g", "ethernet", "fiber"]
BROWSERS = ["chrome", "firefox", "safari", "edge", "opera", "brave", "samsung-internet"]
OS_NAMES = ["Windows 11", "Windows 10", "macOS Sonoma", "macOS Ventura", "iOS 17", "Android 14", "Android 13", "Ubuntu 22.04", "ChromeOS"]
SDK_VERSIONS = ["1.2.0", "1.3.0", "1.4.0", "1.5.0-beta", "2.0.0-rc1"]
CONTENT_GROUPS = ["homepage", "product-listing", "product-detail", "cart", "checkout", "search-results", "category-browse", "deals", "account", "help"]
MARKETING_CHANNELS = ["organic-search", "paid-search", "social-organic", "social-paid", "email", "direct", "referral", "display", "affiliate", "video"]
CUSTOMER_SEGMENTS = ["new-visitor", "returning-customer", "high-value", "at-risk-churn", "dormant", "loyalty-member", "wholesale", "employee", "influencer"]
FEATURE_FLAGS_ACTIVE = ["new-checkout", "ai-recommendations", "dynamic-pricing", "express-shipping", "wishlist-v2", "social-proof", "ar-preview", "voice-search", "crypto-payment", "subscription-model"]

def generate_session_attributes():
    """Generate a rich set of session-level attributes for baggage propagation."""
    session_id = str(uuid.uuid4())
    user_id = f"user-{random.randint(10000, 99999)}"
    device_id = str(uuid.uuid4())
    visitor_id = hashlib.sha256(f"{user_id}-{device_id}".encode()).hexdigest()[:24]

    attrs = {
        "session.id": session_id,
        "synthetic_request": "true",
        "user.id": user_id,
        "user.anonymous_id": visitor_id,
        "user.tier": random.choice(USER_TIERS),
        "user.locale": random.choice(LOCALES),
        "user.country": random.choice(COUNTRIES),
        "user.timezone": random.choice(TIMEZONES),
        "user.account_age_days": str(random.choice([0, 1, 7, 30, 90, 180, 365, 730, 1460])),
        "user.total_orders": str(random.randint(0, 500)),
        "user.lifetime_value_usd": str(round(random.uniform(0, 25000), 2)),
        "user.segment": random.choice(CUSTOMER_SEGMENTS),
        "device.id": device_id,
        "device.type": random.choice(DEVICE_TYPES),
        "device.screen_resolution": random.choice(SCREEN_RESOLUTIONS),
        "device.user_agent": random.choice(USER_AGENTS),
        "client.platform": random.choice(PLATFORMS),
        "client.sdk_version": random.choice(SDK_VERSIONS),
        "client.browser": random.choice(BROWSERS),
        "client.os": random.choice(OS_NAMES),
        "network.type": random.choice(NETWORK_TYPES),
        "network.effective_bandwidth_mbps": str(random.choice([1, 5, 10, 25, 50, 100, 250, 500, 1000])),
        "traffic.referrer": random.choice(REFERRERS),
        "traffic.campaign_id": random.choice(CAMPAIGN_IDS) if random.random() < 0.4 else "",
        "traffic.channel": random.choice(MARKETING_CHANNELS),
        "traffic.landing_page": random.choice(["/", "/deals", "/new-arrivals", "/category/telescopes", "/product/featured"]),
        "experiment.group": random.choice(["control", "variant-a", "variant-b", "variant-c"]),
        "experiment.id": random.choice(AB_TESTS),
        "experiment.allocation_id": str(random.randint(1, 1000)),
        "feature_flags.active": ",".join(random.sample(FEATURE_FLAGS_ACTIVE, random.randint(1, 5))),
        "content.group": random.choice(CONTENT_GROUPS),
        "geo.city": random.choice(["New York", "San Francisco", "London", "Berlin", "Tokyo", "Sydney", "Toronto", "Mumbai", "Seoul", "Paris", "Amsterdam", "Stockholm"]),
        "geo.region": random.choice(["NY", "CA", "TX", "WA", "FL", "IL", "Greater London", "Île-de-France", "Bavaria", "Tokyo", "NSW", "Ontario"]),
        "geo.postal_code": str(random.randint(10000, 99999)),
        "geo.latitude": str(round(random.uniform(-90, 90), 4)),
        "geo.longitude": str(round(random.uniform(-180, 180), 4)),
        "privacy.consent_analytics": random.choice(["true", "false"]),
        "privacy.consent_marketing": random.choice(["true", "false"]),
        "privacy.do_not_track": random.choice(["true", "false"]),
        "request.correlation_id": str(uuid.uuid4()),
        "request.idempotency_key": str(uuid.uuid4()),
        "performance.page_load_ms": str(random.randint(200, 8000)),
        "performance.dns_lookup_ms": str(random.randint(1, 150)),
        "performance.tcp_connect_ms": str(random.randint(5, 200)),
        "performance.ttfb_ms": str(random.randint(50, 2000)),
    }
    return attrs

LOG_LEVELS_CONTEXT = ["verbose", "standard", "minimal"]
HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
RESPONSE_FORMATS = ["json", "protobuf", "html", "xml"]
CACHE_STATUSES = ["hit", "miss", "stale", "bypass", "revalidated"]
CONNECTION_POOLS = ["pool-a", "pool-b", "pool-c", "pool-d"]
TLS_VERSIONS = ["TLSv1.2", "TLSv1.3"]
DNS_RESOLVERS = ["system", "cloudflare-1.1.1.1", "google-8.8.8.8", "internal"]
RETRY_POLICIES = ["exponential-backoff", "linear", "fixed-delay", "none"]
CIRCUIT_STATES = ["closed", "closed", "closed", "half-open", "open"]
LOAD_BALANCER_ALGOS = ["round-robin", "least-connections", "weighted", "ip-hash", "random"]
COMPRESSION_TYPES = ["gzip", "br", "zstd", "none", "deflate"]
AUTH_TOKEN_TYPES = ["bearer-jwt", "session-cookie", "api-key", "oauth2-token"]
REQUEST_PRIORITIES = ["critical", "high", "normal", "low", "background"]
ERROR_CATEGORIES = ["none", "client-error", "server-error", "timeout", "network", "rate-limit"]
DEPLOYMENT_STAGES = ["canary", "blue-green-active", "blue-green-standby", "rolling", "stable"]

def generate_log_context(action, **extra):
    """Generate rich structured log context for any action."""
    ctx = {
        "log.action": action,
        "log.request_id": str(uuid.uuid4()),
        "log.correlation_id": str(uuid.uuid4()),
        "log.trace_context": f"00-{uuid.uuid4().hex}-{uuid.uuid4().hex[:16]}-01",
        "log.timestamp_unix_ms": int(time.time() * 1000),
        "log.level_context": random.choice(LOG_LEVELS_CONTEXT),

        # HTTP context
        "http.request_id": str(uuid.uuid4()),
        "http.method": random.choice(HTTP_METHODS),
        "http.response_format": random.choice(RESPONSE_FORMATS),
        "http.compression": random.choice(COMPRESSION_TYPES),
        "http.keep_alive": random.choice([True, False]),
        "http.request_size_bytes": random.randint(50, 50000),
        "http.response_size_bytes": random.randint(100, 500000),
        "http.response_time_ms": random.randint(5, 5000),
        "http.status_code": random.choice([200, 200, 200, 200, 201, 204, 301, 400, 404, 500, 502, 503]),
        "http.retry_count": random.choice([0, 0, 0, 0, 1, 2, 3]),
        "http.retry_policy": random.choice(RETRY_POLICIES),
        "http.tls_version": random.choice(TLS_VERSIONS),

        # Network context
        "net.connection_pool": random.choice(CONNECTION_POOLS),
        "net.connection_reused": random.choice([True, True, True, False]),
        "net.dns_resolver": random.choice(DNS_RESOLVERS),
        "net.dns_lookup_ms": random.randint(0, 150),
        "net.tcp_connect_ms": random.randint(1, 100),
        "net.tls_handshake_ms": random.randint(5, 200),
        "net.peer_address": f"10.0.{random.randint(0, 255)}.{random.randint(1, 254)}",
        "net.peer_port": random.choice([80, 443, 8080, 8443, 3000, 4317, 50051, 9090]),

        # Client context
        "client.user_agent_family": random.choice(BROWSERS),
        "client.platform": random.choice(PLATFORMS),
        "client.sdk_version": random.choice(SDK_VERSIONS),
        "client.device_type": random.choice(DEVICE_TYPES),
        "client.screen_resolution": random.choice(SCREEN_RESOLUTIONS),
        "client.locale": random.choice(LOCALES),
        "client.timezone": random.choice(TIMEZONES),

        # User context
        "user.id_hash": hashlib.sha256(str(random.randint(1, 100000)).encode()).hexdigest()[:12],
        "user.session_id": str(uuid.uuid4()),
        "user.segment": random.choice(CUSTOMER_SEGMENTS),
        "user.tier": random.choice(USER_TIERS),
        "user.auth_type": random.choice(AUTH_TOKEN_TYPES),
        "user.is_authenticated": random.choice([True, True, True, False]),
        "user.country": random.choice(COUNTRIES),

        # Infrastructure context
        "infra.instance_id": f"load-gen-{random.randint(0, 9)}",
        "infra.deployment_stage": random.choice(DEPLOYMENT_STAGES),
        "infra.load_balancer_algo": random.choice(LOAD_BALANCER_ALGOS),
        "infra.circuit_breaker_state": random.choice(CIRCUIT_STATES),
        "infra.queue_depth": random.randint(0, 100),
        "infra.active_connections": random.randint(1, 500),
        "infra.memory_usage_mb": random.randint(64, 2048),
        "infra.cpu_usage_pct": round(random.uniform(1, 95), 1),
        "infra.gc_pause_ms": round(random.uniform(0, 50), 2),
        "infra.thread_count": random.randint(4, 64),

        # Request priority and routing
        "request.priority": random.choice(REQUEST_PRIORITIES),
        "request.error_category": random.choice(ERROR_CATEGORIES),
        "request.cache_status": random.choice(CACHE_STATUSES),
        "request.rate_limit_remaining": random.randint(0, 10000),
        "request.rate_limit_bucket": f"loadgen-{random.choice(['browse', 'cart', 'checkout', 'search', 'ads'])}",
        "request.idempotency_key": str(uuid.uuid4()),
        "request.feature_flags": ",".join(random.sample(FEATURE_FLAGS_ACTIVE, random.randint(0, 3))),

        # Observability metadata
        "otel.library.name": "load-generator",
        "otel.library.version": "1.4.0",
        "otel.signal": "log",
        "otel.dropped_attributes_count": random.choice([0, 0, 0, 0, 1, 2]),
        "otel.dropped_events_count": 0,
    }
    ctx.update(extra)
    return ctx

categories = [
    "binoculars",
    "telescopes",
    "accessories",
    "assembly",
    "travel",
    "books",
    None,
]

products = [
    "0PUK6V6EV0",
    "1YMWWN1N4O",
    "2ZYFJ3GM2N",
    "66VCHSJNUP",
    "6E92ZMYYFZ",
    "9SIQT8TOJO",
    "L9ECAV7KIM",
    "LS4PSXUNUM",
    "OLJCESPC7Z",
    "HQTGWGPNH4",
]

people_file = open('people.json')
people = json.load(people_file)


def format_card_number(number: str) -> str:
    return re.sub(r'(\d{4})(?=\d)', r'\1-', number)

def get_luhn_check_digit(number_without_check_digit: str) -> str:
    digits = [int(d) for d in (number_without_check_digit + '0')][::-1]
    total = 0

    for i, digit in enumerate(digits):
        if i % 2 == 1:  # double every second digit from the right
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit

    mod = total % 10
    return '0' if mod == 0 else str(10 - mod)

def generate_valid_visa_number() -> str:
    number = '4' + ''.join(str(random.randint(0, 9)) for _ in range(14))
    check_digit = get_luhn_check_digit(number)
    return format_card_number(number + check_digit)

def generate_valid_mastercard_number() -> str:
    bin_prefixes = ['2221', '2222', '2223', '2230', '5100', '5200', '5300', '5400', '5500']
    prefix = random.choice(bin_prefixes)
    number = prefix + ''.join(str(random.randint(0, 9)) for _ in range(15 - len(prefix)))
    check_digit = get_luhn_check_digit(number)
    return format_card_number(number + check_digit)

def generate_credit_card() -> str:
    if random.random() < 0.8:
        return generate_valid_visa_number()
    else:
        return generate_valid_mastercard_number()


class WebsiteUser(HttpUser):
    wait_time = between(1, 10)

    @task(1)
    def index(self):
        logging.info("Loading homepage", extra=generate_log_context("page_view",
            **{"page.name": "homepage", "page.path": "/", "page.type": "landing",
               "page.version": random.choice(["v1", "v2", "v3"]),
               "page.personalized": random.choice([True, False]),
               "page.ab_variant": random.choice(["control", "variant-a", "variant-b"]),
               "content.hero_banner": random.choice(["spring-sale", "new-arrivals", "clearance", "featured-product"]),
               "content.sections_loaded": random.randint(3, 8)}))
        self.client.get("/")

    @task(10)
    def browse_product(self):
        product = random.choice(products)
        logging.info("Browsing product", extra=generate_log_context("product_view",
            **{"product.id": product,
               "product.view_source": random.choice(["search", "category", "recommendation", "direct", "ad-click", "homepage"]),
               "product.position_in_list": random.randint(1, 50),
               "product.impression_id": str(uuid.uuid4()),
               "browse.session_products_viewed": random.randint(1, 20),
               "browse.time_on_previous_page_sec": random.randint(2, 300),
               "browse.scroll_depth_pct": random.randint(0, 100),
               "browse.images_loaded": random.randint(1, 10),
               "browse.reviews_expanded": random.choice([True, False]),
               "browse.comparison_mode": random.choice([True, False])}))
        self.client.get("/api/products/" + product)

    @task(3)
    def get_recommendations(self):
        product = random.choice(products)
        params = {
            "productIds": [product],
        }
        logging.info("Fetching recommendations", extra=generate_log_context("get_recommendations",
            **{"recommendation.seed_product": product,
               "recommendation.context": random.choice(["pdp", "cart", "homepage", "checkout", "post-purchase"]),
               "recommendation.max_results": random.choice([5, 10, 15, 20]),
               "recommendation.algorithm_hint": random.choice(["similar", "complementary", "trending", "personalized"]),
               "recommendation.exclude_owned": random.choice([True, False]),
               "recommendation.price_range_filter": random.choice(["none", "similar", "lower", "higher"]),
               "recommendation.session_rec_requests": random.randint(1, 10)}))
        self.client.get("/api/recommendations", params=params)

    @task(3)
    def get_ads(self):
        category = random.choice(categories)
        params = {
            "contextKeys": [category],
        }
        logging.info("Fetching ads", extra=generate_log_context("get_ads",
            **{"ads.context_key": str(category),
               "ads.placement": random.choice(["sidebar", "banner", "inline", "interstitial"]),
               "ads.page_context": random.choice(["homepage", "product-page", "search", "category", "cart"]),
               "ads.viewport_width": random.choice([320, 768, 1024, 1280, 1440, 1920]),
               "ads.ads_blocked": random.choice([True, False, False, False, False]),
               "ads.consent_given": random.choice([True, True, True, False]),
               "ads.session_impressions": random.randint(0, 50),
               "ads.session_clicks": random.randint(0, 5)}))
        self.client.get("/api/data/", params=params)

    @task(3)
    def view_cart(self):
        logging.info("Viewing cart", extra=generate_log_context("view_cart",
            **{"cart.view_trigger": random.choice(["nav-click", "add-to-cart-redirect", "checkout-back", "deep-link"]),
               "cart.item_count": random.randint(0, 15),
               "cart.estimated_total_usd": round(random.uniform(0, 2000), 2),
               "cart.has_promo_code": random.choice([True, False]),
               "cart.has_out_of_stock": random.choice([True, False, False, False]),
               "cart.currency": random.choice(["USD", "EUR", "GBP", "JPY", "CAD"]),
               "cart.session_cart_views": random.randint(1, 10),
               "cart.last_modified_sec_ago": random.randint(0, 86400)}))
        self.client.get("/api/cart")

    @task(2)
    def add_to_cart(self, user=""):
        if user == "":
            user = str(uuid.uuid1())
        product = random.choice(products)
        quantity = random.choice([1, 2, 3, 4, 5, 10])
        logging.info("Adding to cart", extra=generate_log_context("add_to_cart",
            **{"cart.product_id": product,
               "cart.quantity": quantity,
               "cart.user_id_hash": hashlib.sha256(user.encode()).hexdigest()[:12],
               "cart.action": "add",
               "cart.source": random.choice(["product-page", "quick-add", "wishlist", "recommendation", "reorder"]),
               "cart.existing_items": random.randint(0, 10),
               "cart.product_already_in_cart": random.choice([True, False]),
               "cart.savings_notification_shown": random.choice([True, False]),
               "cart.free_shipping_threshold_met": random.choice([True, False]),
               "cart.upsell_shown": random.choice([True, False]),
               "cart.bundle_eligible": random.choice([True, False])}))
        self.client.get("/api/products/" + product)
        cart_item = {
            "item": {
                "productId": product,
                "quantity": quantity,
            },
            "userId": user,
        }
        self.client.post("/api/cart", json=cart_item)

    @task(1)
    def checkout(self):
        # checkout call with an item added to cart
        user = str(uuid.uuid1())
        self.add_to_cart(user=user)
        checkout_person = random.choice(people)
        checkout_person["userId"] = user
        # generate a valid credit card number
        checkout_person["creditCard"]["creditCardNumber"] = generate_credit_card()
        logging.info("Processing checkout", extra=generate_log_context("checkout",
            **{"checkout.type": "single-item",
               "checkout.user_id_hash": hashlib.sha256(user.encode()).hexdigest()[:12],
               "checkout.items_count": 1,
               "checkout.payment_method": random.choice(["credit-card", "debit-card", "digital-wallet", "buy-now-pay-later"]),
               "checkout.has_shipping_address": True,
               "checkout.has_billing_address": True,
               "checkout.guest_checkout": random.choice([True, False]),
               "checkout.express_checkout": random.choice([True, False]),
               "checkout.promo_applied": random.choice([True, False, False, False]),
               "checkout.estimated_tax_usd": round(random.uniform(0, 50), 2),
               "checkout.estimated_shipping_usd": round(random.uniform(0, 25), 2),
               "checkout.shipping_method": random.choice(["standard", "express", "next-day", "pickup"]),
               "checkout.gift_wrap": random.choice([True, False, False, False, False]),
               "checkout.gift_message": random.choice([True, False, False, False, False, False]),
               "checkout.loyalty_points_used": random.randint(0, 1000),
               "checkout.time_in_cart_sec": random.randint(30, 7200),
               "checkout.page_revisits": random.randint(0, 5)}))
        self.client.post("/api/checkout", json=checkout_person)

    @task(1)
    def checkout_multi(self):
        # checkout call which adds 2-4 different items to cart before checkout
        user = str(uuid.uuid1())
        item_count = random.choice([2, 3, 4])
        for i in range(item_count):
            self.add_to_cart(user=user)
        checkout_person = random.choice(people)
        checkout_person["userId"] = user
        # generate a valid credit card number
        checkout_person["creditCard"]["creditCardNumber"] = generate_credit_card()
        logging.info("Processing multi-item checkout", extra=generate_log_context("checkout_multi",
            **{"checkout.type": "multi-item",
               "checkout.user_id_hash": hashlib.sha256(user.encode()).hexdigest()[:12],
               "checkout.items_count": item_count,
               "checkout.payment_method": random.choice(["credit-card", "debit-card", "digital-wallet", "buy-now-pay-later"]),
               "checkout.has_shipping_address": True,
               "checkout.has_billing_address": True,
               "checkout.guest_checkout": random.choice([True, False]),
               "checkout.express_checkout": False,
               "checkout.promo_applied": random.choice([True, False, False]),
               "checkout.estimated_tax_usd": round(random.uniform(0, 150), 2),
               "checkout.estimated_shipping_usd": round(random.uniform(0, 50), 2),
               "checkout.shipping_method": random.choice(["standard", "express", "next-day", "pickup"]),
               "checkout.split_shipment": random.choice([True, False]),
               "checkout.gift_wrap": random.choice([True, False, False]),
               "checkout.loyalty_points_used": random.randint(0, 5000),
               "checkout.time_in_cart_sec": random.randint(60, 14400),
               "checkout.page_revisits": random.randint(0, 8),
               "checkout.cart_modifications": random.randint(0, 10)}))
        self.client.post("/api/checkout", json=checkout_person)

    @task(5)
    def flood_home(self):
        for _ in range(0, get_flagd_value("loadGeneratorFloodHomepage")):
            logging.info("Flood homepage request", extra=generate_log_context("flood_home",
                **{"flood.iteration": _, "flood.feature_flag": "loadGeneratorFloodHomepage"}))
            self.client.get("/")

    def on_start(self):
        session_attrs = generate_session_attributes()
        ctx = context.get_current()
        for key, value in session_attrs.items():
            ctx = baggage.set_baggage(key, value, context=ctx)
        context.attach(ctx)
        self.index()


browser_traffic_enabled = os.environ.get("LOCUST_BROWSER_TRAFFIC_ENABLED", "").lower() in ("true", "yes", "on")

if browser_traffic_enabled:
    class WebsiteBrowserUser(PlaywrightUser):
        headless = True  # to use a headless browser, without a GUI

        @task
        @pw
        async def open_cart_page_and_change_currency(self, page: PageWithRetry):
            try:
                page.on("console", lambda msg: print(msg.text))
                await page.route('**/*', add_baggage_header)
                await page.goto("/cart", wait_until="domcontentloaded")
                await page.select_option('[name="currency_code"]', 'CHF')
                await page.wait_for_timeout(2000)  # giving the browser time to export the traces
            except:
                pass

        @task
        @pw
        async def add_product_to_cart(self, page: PageWithRetry):
            try:
                page.on("console", lambda msg: print(msg.text))
                await page.route('**/*', add_baggage_header)
                await page.goto("/", wait_until="domcontentloaded")
                await page.click('p:has-text("Roof Binoculars")', wait_until="domcontentloaded")
                await page.click('button:has-text("Add To Cart")', wait_until="domcontentloaded")
                await page.wait_for_timeout(2000)  # giving the browser time to export the traces
            except:
                pass


async def add_baggage_header(route: Route, request: Request):
    existing_baggage = request.headers.get('baggage', '')
    headers = {
        **request.headers,
        'baggage': ', '.join(filter(None, (existing_baggage, 'synthetic_request=true')))
    }
    await route.continue_(headers=headers)
