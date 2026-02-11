#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


# Python
import os
import random
from concurrent import futures

# Pip
import grpc

from openfeature import api
from openfeature.contrib.provider.flagd import FlagdProvider

# Local
import logging
import demo_pb2
import demo_pb2_grpc
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc

from metrics import init_metrics
from logger import getJSONLogger

cached_ids = []
first_run = True

class RecommendationService(demo_pb2_grpc.RecommendationServiceServicer):
    def ListRecommendations(self, request, context):
        prod_list = get_product_list(request.product_ids)
        logger.info(f"Receive ListRecommendations for product ids:{prod_list}")

        # build and return response
        response = demo_pb2.ListRecommendationsResponse()
        response.product_ids.extend(prod_list)

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
    max_responses = 5

    # Formulate the list of characters to list of strings
    request_product_ids_str = ''.join(request_product_ids)
    request_product_ids = request_product_ids_str.split(',')

    # Feature flag scenario - Cache Leak
    if check_feature_flag("recommendationCacheFailure"):
        if random.random() < 0.5 or first_run:
            first_run = False
            logger.info("get_product_list: cache miss")
            cat_response = product_catalog_stub.ListProducts(demo_pb2.Empty())
            response_ids = [x.id for x in cat_response.products]
            cached_ids = cached_ids + response_ids
            cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
            product_ids = cached_ids
        else:
            logger.info("get_product_list: cache hit")
            product_ids = cached_ids
    else:
        cat_response = product_catalog_stub.ListProducts(demo_pb2.Empty())
        product_ids = [x.id for x in cat_response.products]

    # Create a filtered list of products excluding the products received as input
    filtered_products = list(set(product_ids) - set(request_product_ids))
    num_products = len(filtered_products)
    num_return = min(max_responses, num_products)

    # Sample list of indicies to return
    indices = random.sample(range(num_products), num_return)
    # Fetch product ids from indices
    prod_list = [filtered_products[i] for i in indices]

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
    service_name = os.environ.get('OTEL_SERVICE_NAME', 'recommendation')
    api.set_provider(FlagdProvider(host=os.environ.get('FLAGD_HOST', 'flagd'), port=os.environ.get('FLAGD_PORT', 8013)))

    # Initialize metrics (now returns empty dict)
    rec_svc_metrics = init_metrics()

    # Initialize logger
    logger = getJSONLogger('recommendation')

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
