#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


# Python
import os
import random
import uuid
import time
import hashlib
from concurrent import futures

# Pip
import grpc
from opentelemetry import trace, metrics
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
    OTLPLogExporter,
)
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from openfeature import api
from openfeature.contrib.provider.flagd import FlagdProvider

from openfeature.contrib.hook.opentelemetry import TracingHook

# Local
import logging
import demo_pb2
import demo_pb2_grpc
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc

from metrics import (
    init_metrics
)

cached_ids = []
first_run = True

# Recommendation algorithm variants
ML_MODELS = ["collaborative-filtering-v3", "content-based-v2", "hybrid-v1", "deep-learning-v4", "graph-neural-v1", "transformer-v2"]
RECOMMENDATION_STRATEGIES = ["personalized", "trending", "similar-items", "frequently-bought-together", "recently-viewed", "editorial-picks", "seasonal"]
USER_SEGMENTS = ["new-visitor", "returning-customer", "high-value", "bargain-seeker", "researcher", "impulse-buyer", "loyalty-member"]
DIVERSITY_STRATEGIES = ["category-spread", "price-spread", "brand-diversity", "novelty-mix", "popularity-balanced"]
FEATURE_STORES = ["redis-features", "feast-online", "tecton-realtime", "local-cache", "precomputed-batch"]
A_B_VARIANTS = ["control", "variant-a", "variant-b", "variant-c", "variant-d", "holdout"]
RANKING_FACTORS = ["relevance", "margin", "inventory-priority", "promotion-boost", "personalization", "freshness"]
DATA_FRESHNESS = ["real-time", "near-real-time", "hourly-batch", "daily-batch"]
EXPLANATION_TYPES = ["similar-to-viewed", "popular-in-category", "users-also-bought", "trending-now", "based-on-history", "editorial-choice"]
INVENTORY_ZONES = ["us-east", "us-west", "eu-central", "ap-northeast", "ap-southeast"]
CACHE_TIERS = ["l1-local", "l2-redis", "l3-memcached", "origin"]


class RecommendationService(demo_pb2_grpc.RecommendationServiceServicer):
    def ListRecommendations(self, request, context):
        request_start = time.time()
        prod_list = get_product_list(request.product_ids)
        span = trace.get_current_span()
        span.set_attribute("app.products_recommended.count", len(prod_list))

        # Rich ML/recommendation attributes
        model = random.choice(ML_MODELS)
        strategy = random.choice(RECOMMENDATION_STRATEGIES)
        inference_time = random.uniform(5, 200)
        span.set_attribute("app.ml.model_name", model)
        span.set_attribute("app.ml.model_version", f"{model}-{random.randint(1,20)}.{random.randint(0,99)}")
        span.set_attribute("app.ml.model_sha", hashlib.sha256(model.encode()).hexdigest()[:16])
        span.set_attribute("app.ml.inference_time_ms", round(inference_time, 2))
        span.set_attribute("app.ml.batch_size", len(request.product_ids) if request.product_ids else 1)
        span.set_attribute("app.ml.feature_count", random.randint(50, 500))
        span.set_attribute("app.ml.feature_store", random.choice(FEATURE_STORES))
        span.set_attribute("app.ml.feature_fetch_time_ms", round(random.uniform(1, 50), 2))
        span.set_attribute("app.ml.confidence_score", round(random.uniform(0.3, 0.99), 4))
        span.set_attribute("app.ml.prediction_variance", round(random.uniform(0.01, 0.3), 4))
        span.set_attribute("app.ml.gpu_utilized", random.choice([True, False]))
        span.set_attribute("app.ml.tensor_memory_mb", random.randint(50, 2000))
        span.set_attribute("app.ml.quantization", random.choice(["none", "int8", "fp16", "bf16"]))

        # Recommendation strategy attributes
        span.set_attribute("app.recommendation.strategy", strategy)
        span.set_attribute("app.recommendation.diversity_method", random.choice(DIVERSITY_STRATEGIES))
        span.set_attribute("app.recommendation.diversity_score", round(random.uniform(0.1, 1.0), 3))
        span.set_attribute("app.recommendation.novelty_score", round(random.uniform(0.0, 1.0), 3))
        span.set_attribute("app.recommendation.serendipity_score", round(random.uniform(0.0, 0.5), 3))
        span.set_attribute("app.recommendation.coverage_score", round(random.uniform(0.05, 0.8), 3))
        span.set_attribute("app.recommendation.candidates_generated", random.randint(50, 5000))
        span.set_attribute("app.recommendation.candidates_after_filter", random.randint(10, 500))
        span.set_attribute("app.recommendation.final_count", len(prod_list))
        span.set_attribute("app.recommendation.ranking_factor", random.choice(RANKING_FACTORS))
        span.set_attribute("app.recommendation.explanation_type", random.choice(EXPLANATION_TYPES))
        span.set_attribute("app.recommendation.request_id", str(uuid.uuid4()))

        # A/B testing attributes
        span.set_attribute("app.experiment.variant", random.choice(A_B_VARIANTS))
        span.set_attribute("app.experiment.name", f"rec-{strategy}-test")
        span.set_attribute("app.experiment.allocation_pct", random.choice([1, 5, 10, 25, 50]))
        span.set_attribute("app.experiment.is_holdout", random.random() < 0.05)

        # User context attributes
        span.set_attribute("app.user.segment", random.choice(USER_SEGMENTS))
        span.set_attribute("app.user.interaction_count_30d", random.randint(0, 200))
        span.set_attribute("app.user.purchase_count_30d", random.randint(0, 20))
        span.set_attribute("app.user.avg_session_duration_sec", random.randint(30, 3600))
        span.set_attribute("app.user.preference_categories", ",".join(random.sample(["binoculars", "telescopes", "accessories", "books", "travel", "optics"], random.randint(1, 4))))
        span.set_attribute("app.user.price_sensitivity", random.choice(["low", "medium", "high"]))
        span.set_attribute("app.user.brand_affinity", random.choice(["none", "weak", "strong"]))

        # Cache and performance attributes
        span.set_attribute("app.cache.tier_hit", random.choice(CACHE_TIERS))
        span.set_attribute("app.cache.hit", random.random() < 0.6)
        span.set_attribute("app.cache.key", hashlib.sha256(",".join(request.product_ids).encode()).hexdigest()[:24])
        span.set_attribute("app.cache.ttl_sec", random.choice([60, 300, 600, 1800, 3600]))
        span.set_attribute("app.cache.size_bytes", random.randint(100, 50000))

        # Inventory and availability context
        span.set_attribute("app.inventory.zone", random.choice(INVENTORY_ZONES))
        span.set_attribute("app.inventory.check_time_ms", round(random.uniform(1, 30), 2))
        span.set_attribute("app.inventory.out_of_stock_filtered", random.randint(0, 5))

        # Data freshness
        span.set_attribute("app.data.freshness", random.choice(DATA_FRESHNESS))
        span.set_attribute("app.data.pipeline_lag_sec", random.randint(0, 7200))
        span.set_attribute("app.data.feature_staleness_sec", random.randint(0, 3600))

        # Processing time tracking
        total_time_ms = (time.time() - request_start) * 1000
        span.set_attribute("app.processing.total_time_ms", round(total_time_ms, 2))
        span.set_attribute("app.processing.overhead_ms", round(total_time_ms - inference_time, 2))

        # Extended ML pipeline attributes
        span.set_attribute("app.ml.embedding_dim", random.choice([64, 128, 256, 512, 768, 1024]))
        span.set_attribute("app.ml.embedding_lookup_ms", round(random.uniform(0.5, 20), 2))
        span.set_attribute("app.ml.nearest_neighbors_k", random.randint(10, 200))
        span.set_attribute("app.ml.similarity_metric", random.choice(["cosine", "euclidean", "dot-product", "jaccard"]))
        span.set_attribute("app.ml.reranking_applied", random.choice([True, False]))
        span.set_attribute("app.ml.reranking_model", random.choice(["xgboost-v2", "lightgbm-v3", "neural-reranker-v1", "none"]))
        span.set_attribute("app.ml.reranking_time_ms", round(random.uniform(1, 30), 2))
        span.set_attribute("app.ml.cold_start_fallback", random.random() < 0.05)
        span.set_attribute("app.ml.exploration_rate", round(random.uniform(0.01, 0.15), 3))
        span.set_attribute("app.ml.exploitation_rate", round(random.uniform(0.85, 0.99), 3))
        span.set_attribute("app.ml.bandit_arm_selected", random.choice(["popular", "personalized", "diverse", "trending"]))
        span.set_attribute("app.ml.feature_importance_top", random.choice(["purchase_history", "browse_history", "category_affinity", "price_sensitivity", "brand_loyalty"]))
        span.set_attribute("app.ml.model_staleness_hours", random.randint(0, 72))
        span.set_attribute("app.ml.training_data_size", random.randint(100000, 10000000))
        span.set_attribute("app.ml.online_learning_enabled", random.choice([True, False]))

        # Filtering and business rules
        span.set_attribute("app.filter.out_of_stock_removed", random.randint(0, 10))
        span.set_attribute("app.filter.price_range_applied", random.choice([True, False]))
        span.set_attribute("app.filter.brand_exclusion_applied", random.choice([True, False]))
        span.set_attribute("app.filter.age_restriction_applied", random.choice([True, False, False, False]))
        span.set_attribute("app.filter.geo_restriction_applied", random.choice([True, False, False, False]))
        span.set_attribute("app.filter.duplicate_removed", random.randint(0, 5))
        span.set_attribute("app.filter.previously_purchased_removed", random.randint(0, 3))
        span.set_attribute("app.filter.total_rules_evaluated", random.randint(5, 25))
        span.set_attribute("app.filter.total_items_removed", random.randint(0, 20))

        # Personalization context
        span.set_attribute("app.personalization.user_profile_exists", random.choice([True, True, True, False]))
        span.set_attribute("app.personalization.profile_completeness_pct", random.randint(10, 100))
        span.set_attribute("app.personalization.interaction_history_count", random.randint(0, 500))
        span.set_attribute("app.personalization.last_interaction_days_ago", random.randint(0, 365))
        span.set_attribute("app.personalization.preference_categories", ",".join(random.sample(["binoculars", "telescopes", "accessories", "books", "travel", "optics", "mounts"], random.randint(1, 4))))
        span.set_attribute("app.personalization.price_sensitivity_score", round(random.uniform(0, 1), 3))
        span.set_attribute("app.personalization.brand_affinity_top", random.choice(["Celestron", "Meade", "Orion", "Sky-Watcher", "Nikon", "none"]))

        # Quality metrics for the recommendations
        span.set_attribute("app.quality.mean_relevance_score", round(random.uniform(0.3, 0.95), 3))
        span.set_attribute("app.quality.price_diversity_index", round(random.uniform(0.1, 1.0), 3))
        span.set_attribute("app.quality.category_coverage", round(random.uniform(0.1, 0.8), 3))
        span.set_attribute("app.quality.freshness_score", round(random.uniform(0.3, 1.0), 3))
        span.set_attribute("app.quality.popularity_bias", round(random.uniform(0.0, 0.8), 3))

        logger.info(f"Receive ListRecommendations for product ids:{prod_list}", extra={
            "recommendation.operation": "list_recommendations",
            "recommendation.request_id": str(uuid.uuid4()),
            "recommendation.seed_product_ids": ",".join(request.product_ids) if request.product_ids else "",
            "recommendation.result_count": len(prod_list),
            "recommendation.model": model,
            "recommendation.strategy": strategy,
            "recommendation.inference_time_ms": round(inference_time, 2),
            "recommendation.total_time_ms": round(total_time_ms, 2),
            "recommendation.cache_hit": random.random() < 0.6,
            "recommendation.cache_tier": random.choice(CACHE_TIERS),
            "recommendation.candidates_generated": random.randint(50, 5000),
            "recommendation.diversity_score": round(random.uniform(0.1, 1.0), 3),
            "recommendation.user_segment": random.choice(USER_SEGMENTS),
            "recommendation.experiment_variant": random.choice(A_B_VARIANTS),
            "ml.model_version": f"{model}-{random.randint(1,20)}.{random.randint(0,99)}",
            "ml.feature_count": random.randint(50, 500),
            "ml.feature_store": random.choice(FEATURE_STORES),
            "ml.confidence_score": round(random.uniform(0.3, 0.99), 4),
            "ml.gpu_utilized": random.choice([True, False]),
            "ml.batch_size": len(request.product_ids) if request.product_ids else 1,
            "infra.handler_instance": f"recommendation-{random.randint(0, 4)}",
            "infra.memory_usage_mb": random.randint(128, 2048),
            "infra.cpu_pct": round(random.uniform(5, 90), 1),
            "infra.thread_count": random.randint(4, 32),
            "infra.queue_depth": random.randint(0, 50),
            "net.peer_address": f"10.0.{random.randint(0, 255)}.{random.randint(1, 254)}",
            "grpc.method": "/oteldemo.RecommendationService/ListRecommendations",
            "grpc.status_code": "OK",
        })

        # build and return response
        response = demo_pb2.ListRecommendationsResponse()
        response.product_ids.extend(prod_list)

        # Collect metrics for this service
        rec_svc_metrics["app_recommendations_counter"].add(len(prod_list), {'recommendation.type': 'catalog'})

        return response

    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING)

    def Watch(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.UNIMPLEMENTED)


def get_product_list(request_product_ids):
    global first_run
    global cached_ids
    with tracer.start_as_current_span("get_product_list") as span:
        max_responses = 5

        # Formulate the list of characters to list of strings
        request_product_ids_str = ''.join(request_product_ids)
        request_product_ids = request_product_ids_str.split(',')

        # Feature flag scenario - Cache Leak
        if check_feature_flag("recommendationCacheFailure"):
            span.set_attribute("app.recommendation.cache_enabled", True)
            if random.random() < 0.5 or first_run:
                first_run = False
                span.set_attribute("app.cache_hit", False)
                logger.info("get_product_list: cache miss", extra={
                    "cache.operation": "miss",
                    "cache.tier": random.choice(CACHE_TIERS),
                    "cache.key": hashlib.sha256(",".join(request_product_ids).encode()).hexdigest()[:16],
                    "cache.miss_reason": random.choice(["expired", "evicted", "not-found", "cold-start"]),
                    "cache.fetch_time_ms": round(random.uniform(1, 100), 2),
                    "cache.size_before": len(cached_ids),
                    "catalog.fetch_required": True,
                    "catalog.fetch_source": random.choice(["primary-db", "read-replica"]),
                    "catalog.fetch_time_ms": round(random.uniform(10, 200), 2),
                    "infra.handler_instance": f"recommendation-{random.randint(0, 4)}",
                })
                cat_response = product_catalog_stub.ListProducts(demo_pb2.Empty())
                response_ids = [x.id for x in cat_response.products]
                cached_ids = cached_ids + response_ids
                cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                product_ids = cached_ids
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit", extra={
                    "cache.operation": "hit",
                    "cache.tier": random.choice(CACHE_TIERS),
                    "cache.key": hashlib.sha256(",".join(request_product_ids).encode()).hexdigest()[:16],
                    "cache.age_sec": random.randint(1, 3600),
                    "cache.ttl_remaining_sec": random.randint(0, 3600),
                    "cache.size": len(cached_ids),
                    "cache.hit_rate": round(random.uniform(0.5, 0.99), 3),
                    "catalog.fetch_required": False,
                    "infra.handler_instance": f"recommendation-{random.randint(0, 4)}",
                })
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            cat_response = product_catalog_stub.ListProducts(demo_pb2.Empty())
            product_ids = [x.id for x in cat_response.products]

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        num_return = min(max_responses, num_products)

        # Sample list of indicies to return
        indices = random.sample(range(num_products), num_return)
        # Fetch product ids from indices
        prod_list = [filtered_products[i] for i in indices]

        span.set_attribute("app.filtered_products.list", prod_list)

        return prod_list


def must_map_env(key: str):
    value = os.environ.get(key)
    if value is None:
        raise Exception(f'{key} environment variable must be set')
    return value


def check_feature_flag(flag_name: str):
    # Initialize OpenFeature
    client = api.get_client()
    return client.get_boolean_value("recommendationCacheFailure", False)


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    api.set_provider(FlagdProvider(host=os.environ.get('FLAGD_HOST', 'flagd'), port=os.environ.get('FLAGD_PORT', 8013)))
    api.add_hooks([TracingHook()])

    # Initialize Traces and Metrics
    tracer = trace.get_tracer_provider().get_tracer(service_name)
    meter = metrics.get_meter_provider().get_meter(service_name)
    rec_svc_metrics = init_metrics(meter)

    # Initialize Logs
    logger_provider = LoggerProvider(
        resource=Resource.create(
            {
                'service.name': service_name,
            }
        ),
    )
    set_logger_provider(logger_provider)
    log_exporter = OTLPLogExporter(insecure=True)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)

    # Attach OTLP handler to logger
    logger = logging.getLogger('main')
    logger.addHandler(handler)

    catalog_addr = must_map_env('PRODUCT_CATALOG_ADDR')
    pc_channel = grpc.insecure_channel(catalog_addr)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)

    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Add class to gRPC server
    service = RecommendationService()
    demo_pb2_grpc.add_RecommendationServiceServicer_to_server(service, server)
    health_pb2_grpc.add_HealthServicer_to_server(service, server)

    # Start server
    port = must_map_env('RECOMMENDATION_PORT')
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f'Recommendation service started, listening on port {port}')
    server.wait_for_termination()
