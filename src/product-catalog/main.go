// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package main

//go:generate go install google.golang.org/protobuf/cmd/protoc-gen-go
//go:generate go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
//go:generate protoc --go_out=./ --go-grpc_out=./ --proto_path=../../pb ../../pb/demo.proto

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	otelhooks "github.com/open-feature/go-sdk-contrib/hooks/open-telemetry/pkg"
	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
	"github.com/open-feature/go-sdk/openfeature"
	pb "github.com/opentelemetry/opentelemetry-demo/src/product-catalog/genproto/oteldemo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	log               *logrus.Logger
	catalog           []*pb.Product
	resource          *sdkresource.Resource
	initResourcesOnce sync.Once
)

const DEFAULT_RELOAD_INTERVAL = 10

func init() {
	log = logrus.New()

	loadProductCatalog()
}

func initResource() *sdkresource.Resource {
	initResourcesOnce.Do(func() {
		extraResources, _ := sdkresource.New(
			context.Background(),
			sdkresource.WithOS(),
			sdkresource.WithProcess(),
			sdkresource.WithContainer(),
			sdkresource.WithHost(),
		)
		resource, _ = sdkresource.Merge(
			sdkresource.Default(),
			extraResources,
		)
	})
	return resource
}

func initTracerProvider() *sdktrace.TracerProvider {
	ctx := context.Background()

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		log.Fatalf("OTLP Trace gRPC Creation: %v", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(initResource()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp
}

func initMeterProvider() *sdkmetric.MeterProvider {
	ctx := context.Background()

	exporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		log.Fatalf("new otlp metric grpc exporter failed: %v", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
		sdkmetric.WithResource(initResource()),
	)
	otel.SetMeterProvider(mp)
	return mp
}

func main() {
	tp := initTracerProvider()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Fatalf("Tracer Provider Shutdown: %v", err)
		}
		log.Println("Shutdown tracer provider")
	}()

	mp := initMeterProvider()
	defer func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			log.Fatalf("Error shutting down meter provider: %v", err)
		}
		log.Println("Shutdown meter provider")
	}()
	openfeature.AddHooks(otelhooks.NewTracesHook())
	err := openfeature.SetProvider(flagd.NewProvider())
	if err != nil {
		log.Fatal(err)
	}

	err = runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second))
	if err != nil {
		log.Fatal(err)
	}

	svc := &productCatalog{}
	var port string
	mustMapEnv(&port, "PRODUCT_CATALOG_PORT")

	log.Infof("Product Catalog gRPC server started on port: %s", port)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("TCP Listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	reflection.Register(srv)

	pb.RegisterProductCatalogServiceServer(srv, svc)
	healthpb.RegisterHealthServer(srv, svc)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	defer cancel()

	go func() {
		if err := srv.Serve(ln); err != nil {
			log.Fatalf("Failed to serve gRPC server, err: %v", err)
		}
	}()

	<-ctx.Done()

	srv.GracefulStop()
	log.Println("Product Catalog gRPC server stopped")
}

type productCatalog struct {
	pb.UnimplementedProductCatalogServiceServer
}

func loadProductCatalog() {
	log.Info("Loading Product Catalog...")
	var err error
	catalog, err = readProductFiles()
	if err != nil {
		log.Fatalf("Error reading product files: %v\n", err)
		os.Exit(1)
	}

	// Default reload interval is 10 seconds
	interval := DEFAULT_RELOAD_INTERVAL
	si := os.Getenv("PRODUCT_CATALOG_RELOAD_INTERVAL")
	if si != "" {
		interval, _ = strconv.Atoi(si)
		if interval <= 0 {
			interval = DEFAULT_RELOAD_INTERVAL
		}
	}
	log.Infof("Product Catalog reload interval: %d", interval)

	ticker := time.NewTicker(time.Duration(interval) * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				log.Info("Reloading Product Catalog...")
				catalog, err = readProductFiles()
				if err != nil {
					log.Errorf("Error reading product files: %v", err)
					continue
				}
			}
		}
	}()
}

func readProductFiles() ([]*pb.Product, error) {

	// find all .json files in the products directory
	entries, err := os.ReadDir("./products")
	if err != nil {
		return nil, err
	}

	jsonFiles := make([]fs.FileInfo, 0, len(entries))
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".json") {
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			jsonFiles = append(jsonFiles, info)
		}
	}

	// read the contents of each .json file and unmarshal into a ListProductsResponse
	// then append the products to the catalog
	var products []*pb.Product
	for _, f := range jsonFiles {
		jsonData, err := os.ReadFile("./products/" + f.Name())
		if err != nil {
			return nil, err
		}

		var res pb.ListProductsResponse
		if err := protojson.Unmarshal(jsonData, &res); err != nil {
			return nil, err
		}

		products = append(products, res.Products...)
	}

	log.WithFields(logrus.Fields{
		"catalog.product_count":     len(products),
		"catalog.load_source":       "filesystem",
		"catalog.directory":         "./products",
		"catalog.format":            "json",
		"catalog.schema_version":    "v2",
		"catalog.load_timestamp_ms": time.Now().UnixMilli(),
		"catalog.validation":        "passed",
		"infra.instance_id":         fmt.Sprintf("product-catalog-%d", rand.Intn(5)),
		"infra.memory_after_mb":     rand.Intn(200) + 50,
	}).Infof("Loaded %d products", len(products))

	return products, nil
}

func mustMapEnv(target *string, key string) {
	value, present := os.LookupEnv(key)
	if !present {
		log.Fatalf("Environment Variable Not Set: %q", key)
	}
	*target = value
}

func (p *productCatalog) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (p *productCatalog) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "health check via Watch not implemented")
}

var (
	catalogCategories   = []string{"binoculars", "telescopes", "accessories", "assembly", "travel", "books", "optics", "mounts", "eyepieces", "filters"}
	catalogBrands       = []string{"Celestron", "Meade", "Orion", "Sky-Watcher", "Vixen", "Bresser", "Nikon", "Canon", "Bushnell", "Leica"}
	sortOrders          = []string{"relevance", "price-asc", "price-desc", "rating", "newest", "bestselling", "name-asc"}
	catalogSources      = []string{"primary-db", "read-replica", "cache-l1", "cache-l2", "cdn-edge"}
	priceRanges         = []string{"budget", "mid-range", "premium", "luxury", "professional"}
	productConditions   = []string{"new", "refurbished", "open-box", "used-like-new"}
	recommendationTypes = []string{"similar", "frequently-bought-together", "customers-also-viewed", "trending", "personalized"}
	indexVersions       = []string{"v2024.1", "v2024.2", "v2024.3", "v2025.1"}
)

func sha256Short(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])[:16]
}

func (p *productCatalog) ListProducts(ctx context.Context, req *pb.Empty) (*pb.ListProductsResponse, error) {
	span := trace.SpanFromContext(ctx)

	log.WithFields(logrus.Fields{
		"catalog.operation":           "list_products",
		"catalog.product_count":       len(catalog),
		"catalog.request_id":          fmt.Sprintf("req-%d", rand.Int63()),
		"catalog.source":              catalogSources[rand.Intn(len(catalogSources))],
		"catalog.cache_hit":           rand.Float64() < 0.7,
		"catalog.cache_age_sec":       rand.Intn(300),
		"catalog.index_version":       indexVersions[rand.Intn(len(indexVersions))],
		"catalog.response_size_bytes": rand.Intn(50000) + 5000,
		"catalog.serialization_ms":    rand.Intn(20) + 1,
		"catalog.compression":         []string{"gzip", "none", "snappy", "zstd"}[rand.Intn(4)],
		"catalog.pagination_offset":   0,
		"catalog.pagination_limit":    100,
		"catalog.filter_active":       false,
		"infra.handler_instance":      fmt.Sprintf("product-catalog-%d", rand.Intn(5)),
		"infra.goroutine_count":       rand.Intn(200) + 20,
		"infra.heap_alloc_mb":         rand.Intn(100) + 20,
		"infra.deployment_slot":       []string{"blue", "green"}[rand.Intn(2)],
		"net.peer_address":            fmt.Sprintf("10.0.%d.%d", rand.Intn(256), rand.Intn(254)+1),
		"grpc.method":                 "/oteldemo.ProductCatalogService/ListProducts",
		"grpc.status_code":            "OK",
	}).Info("Listing all products")

	span.SetAttributes(
		attribute.Int("app.products.count", len(catalog)),
		// Catalog metadata
		attribute.String("app.catalog.version", fmt.Sprintf("v%d", time.Now().Unix()/3600)),
		attribute.String("app.catalog.source", catalogSources[rand.Intn(len(catalogSources))]),
		attribute.Int("app.catalog.total_products", len(catalog)),
		attribute.Int("app.catalog.active_products", len(catalog)-rand.Intn(2)),
		attribute.Int64("app.catalog.last_updated_epoch", time.Now().Add(-time.Duration(rand.Intn(3600))*time.Second).Unix()),
		attribute.String("app.catalog.index_version", indexVersions[rand.Intn(len(indexVersions))]),
		attribute.Int("app.catalog.query_time_ms", rand.Intn(50)+1),
		attribute.Bool("app.catalog.from_cache", rand.Float64() < 0.7),
		attribute.Int("app.catalog.cache_ttl_remaining_sec", rand.Intn(300)),
		attribute.String("app.catalog.cache_key", sha256Short("list-all-products")),
		// Category distribution
		attribute.Int("app.catalog.categories_count", len(catalogCategories)),
		attribute.Int("app.catalog.brands_count", len(catalogBrands)),
		attribute.String("app.catalog.default_sort", sortOrders[rand.Intn(len(sortOrders))]),
		attribute.String("app.catalog.default_currency", "USD"),
		attribute.Int("app.catalog.page_size", 100),
		attribute.Int("app.catalog.page_number", 1),
		attribute.Bool("app.catalog.has_more_pages", false),
		// Performance attributes
		attribute.Int("app.catalog.serialization_time_ms", rand.Intn(20)+1),
		attribute.Int("app.catalog.response_size_bytes", rand.Intn(50000)+5000),
		attribute.String("app.catalog.compression", []string{"gzip", "none", "snappy", "zstd"}[rand.Intn(4)]),
		// Infrastructure
		attribute.String("app.infra.handler_instance", fmt.Sprintf("product-catalog-%d", rand.Intn(5))),
		attribute.Int("app.infra.goroutine_count", rand.Intn(200)+20),
		attribute.Int64("app.infra.heap_alloc_bytes", int64(rand.Intn(100000000)+5000000)),
		attribute.Bool("app.infra.gc_running", rand.Float64() < 0.05),
		attribute.String("app.infra.deployment_slot", []string{"blue", "green"}[rand.Intn(2)]),
	)
	return &pb.ListProductsResponse{Products: catalog}, nil
}

func (p *productCatalog) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.Product, error) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		attribute.String("app.product.id", req.Id),
	)

	// GetProduct will fail on a specific product when feature flag is enabled
	if p.checkProductFailure(ctx, req.Id) {
		msg := fmt.Sprintf("Error: Product Catalog Fail Feature Flag Enabled")
		span.SetStatus(otelcodes.Error, msg)
		span.AddEvent(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	var found *pb.Product
	for _, product := range catalog {
		if req.Id == product.Id {
			found = product
			break
		}
	}

	if found == nil {
		msg := fmt.Sprintf("Product Not Found: %s", req.Id)
		span.SetStatus(otelcodes.Error, msg)
		span.AddEvent(msg)
		return nil, status.Errorf(codes.NotFound, msg)
	}

	log.WithFields(logrus.Fields{
		"catalog.operation":         "get_product",
		"catalog.product_id":        req.Id,
		"catalog.product_name":      found.Name,
		"catalog.product_found":     true,
		"catalog.source":            catalogSources[rand.Intn(len(catalogSources))],
		"catalog.cache_hit":         rand.Float64() < 0.8,
		"catalog.lookup_time_ms":    rand.Intn(30) + 1,
		"catalog.category":          catalogCategories[rand.Intn(len(catalogCategories))],
		"catalog.brand":             catalogBrands[rand.Intn(len(catalogBrands))],
		"catalog.in_stock":          rand.Float64() > 0.1,
		"catalog.stock_quantity":    rand.Intn(500),
		"catalog.price_usd":         fmt.Sprintf("%d.%02d", found.PriceUsd.GetUnits(), found.PriceUsd.GetNanos()/10000000),
		"catalog.is_featured":       rand.Float64() < 0.15,
		"catalog.is_on_sale":        rand.Float64() < 0.3,
		"catalog.rating":            float64(rand.Intn(50)+1) / 10.0,
		"catalog.reviews_count":     rand.Intn(5000),
		"infra.handler_instance":    fmt.Sprintf("product-catalog-%d", rand.Intn(5)),
		"infra.query_time_ms":       rand.Intn(30) + 1,
		"net.peer_address":          fmt.Sprintf("10.0.%d.%d", rand.Intn(256), rand.Intn(254)+1),
		"grpc.method":               "/oteldemo.ProductCatalogService/GetProduct",
		"grpc.status_code":          "OK",
	}).Info("Product found")

	span.AddEvent("Product Found")
	span.SetAttributes(
		attribute.String("app.product.id", req.Id),
		attribute.String("app.product.name", found.Name),
		attribute.String("app.product.description_hash", sha256Short(found.Description)),
		attribute.Int("app.product.description_length", len(found.Description)),
		attribute.String("app.product.category", catalogCategories[rand.Intn(len(catalogCategories))]),
		attribute.String("app.product.brand", catalogBrands[rand.Intn(len(catalogBrands))]),
		attribute.String("app.product.condition", productConditions[rand.Intn(len(productConditions))]),
		attribute.String("app.product.price_range", priceRanges[rand.Intn(len(priceRanges))]),
		attribute.Float64("app.product.price_usd", float64(found.PriceUsd.GetUnits())+float64(found.PriceUsd.GetNanos())/1e9),
		attribute.String("app.product.currency", found.PriceUsd.GetCurrencyCode()),
		attribute.Float64("app.product.rating", float64(rand.Intn(50)+1)/10.0),
		attribute.Int("app.product.reviews_count", rand.Intn(5000)),
		attribute.Int("app.product.questions_count", rand.Intn(200)),
		attribute.Bool("app.product.in_stock", rand.Float64() > 0.1),
		attribute.Int("app.product.stock_quantity", rand.Intn(500)),
		attribute.Int("app.product.stock_reserved", rand.Intn(50)),
		attribute.String("app.product.warehouse_location", []string{"A1-R3-S7", "B2-R1-S3", "C4-R5-S2", "D1-R2-S8", "E3-R4-S1"}[rand.Intn(5)]),
		attribute.Bool("app.product.is_featured", rand.Float64() < 0.15),
		attribute.Bool("app.product.is_on_sale", rand.Float64() < 0.3),
		attribute.Float64("app.product.sale_discount_pct", float64(rand.Intn(50))),
		attribute.Float64("app.product.weight_kg", rand.Float64()*10+0.1),
		attribute.String("app.product.dimensions_cm", fmt.Sprintf("%dx%dx%d", rand.Intn(50)+5, rand.Intn(30)+5, rand.Intn(20)+3)),
		attribute.Int("app.product.image_count", rand.Intn(10)+1),
		attribute.Bool("app.product.has_video", rand.Float64() < 0.25),
		attribute.Bool("app.product.has_3d_model", rand.Float64() < 0.1),
		attribute.Int("app.product.view_count_30d", rand.Intn(10000)),
		attribute.Int("app.product.purchase_count_30d", rand.Intn(500)),
		attribute.Float64("app.product.conversion_rate", rand.Float64()*0.15),
		attribute.String("app.product.sku", fmt.Sprintf("SKU-%s-%05d", found.Id[:3], rand.Intn(99999))),
		attribute.String("app.product.upc", fmt.Sprintf("%012d", rand.Int63n(999999999999))),
		attribute.String("app.product.manufacturer_part_number", fmt.Sprintf("MPN-%08d", rand.Intn(99999999))),
		attribute.StringSlice("app.product.tags", []string{"astronomy", "outdoor", "optical", "professional"}[:rand.Intn(4)+1]),
		attribute.String("app.product.age_restriction", []string{"none", "none", "none", "13+", "18+"}[rand.Intn(5)]),
		attribute.Bool("app.product.requires_shipping", true),
		attribute.Bool("app.product.is_digital", false),
		attribute.String("app.product.return_policy", []string{"30-day", "60-day", "90-day", "no-returns"}[rand.Intn(4)]),
		attribute.String("app.product.warranty", []string{"1-year", "2-year", "3-year", "lifetime", "none"}[rand.Intn(5)]),
		// Recommendation context
		attribute.String("app.recommendation.type", recommendationTypes[rand.Intn(len(recommendationTypes))]),
		attribute.Int("app.recommendation.rank_position", rand.Intn(20)+1),
		attribute.Float64("app.recommendation.relevance_score", rand.Float64()),
		attribute.String("app.recommendation.model_version", fmt.Sprintf("rec-v%d.%d", rand.Intn(3)+1, rand.Intn(10))),
		// Data source and cache info
		attribute.String("app.data.source", catalogSources[rand.Intn(len(catalogSources))]),
		attribute.Bool("app.data.cache_hit", rand.Float64() < 0.8),
		attribute.Int("app.data.query_time_ms", rand.Intn(30)+1),
		attribute.String("app.data.cache_key", sha256Short(req.Id)),
	)
	return found, nil
}

func (p *productCatalog) SearchProducts(ctx context.Context, req *pb.SearchProductsRequest) (*pb.SearchProductsResponse, error) {
	span := trace.SpanFromContext(ctx)

	var result []*pb.Product
	for _, product := range catalog {
		if strings.Contains(strings.ToLower(product.Name), strings.ToLower(req.Query)) ||
			strings.Contains(strings.ToLower(product.Description), strings.ToLower(req.Query)) {
			result = append(result, product)
		}
	}
	span.SetAttributes(
		attribute.Int("app.products_search.count", len(result)),
		attribute.String("app.search.query", req.Query),
		attribute.Int("app.search.query_length", len(req.Query)),
		attribute.String("app.search.query_hash", sha256Short(req.Query)),
		attribute.Int("app.search.results_count", len(result)),
		attribute.Int("app.search.total_candidates", len(catalog)),
		attribute.Float64("app.search.precision", float64(len(result))/float64(max(len(catalog), 1))),
		attribute.Int("app.search.execution_time_ms", rand.Intn(100)+5),
		attribute.String("app.search.algorithm", []string{"full-text", "fuzzy", "semantic", "hybrid"}[rand.Intn(4)]),
		attribute.String("app.search.index_version", indexVersions[rand.Intn(len(indexVersions))]),
		attribute.Bool("app.search.spell_corrected", rand.Float64() < 0.15),
		attribute.Bool("app.search.used_synonyms", rand.Float64() < 0.3),
		attribute.String("app.search.sort_order", sortOrders[rand.Intn(len(sortOrders))]),
		attribute.Int("app.search.page_number", 1),
		attribute.Int("app.search.page_size", 20),
		attribute.Bool("app.search.has_filters", rand.Float64() < 0.4),
		attribute.String("app.search.filter_category", catalogCategories[rand.Intn(len(catalogCategories))]),
		attribute.String("app.search.filter_brand", catalogBrands[rand.Intn(len(catalogBrands))]),
		attribute.String("app.search.filter_price_range", priceRanges[rand.Intn(len(priceRanges))]),
		attribute.Bool("app.search.filter_in_stock_only", rand.Float64() < 0.5),
		attribute.Float64("app.search.filter_min_rating", float64(rand.Intn(5))),
		attribute.Int("app.search.suggestions_count", rand.Intn(10)),
		attribute.Bool("app.search.is_autocomplete", rand.Float64() < 0.3),
		attribute.String("app.search.source", []string{"search-bar", "category-filter", "voice-search", "visual-search", "url-param"}[rand.Intn(5)]),
		attribute.String("app.search.session_search_count", fmt.Sprintf("%d", rand.Intn(20)+1)),
	)
	return &pb.SearchProductsResponse{Results: result}, nil
}

func (p *productCatalog) checkProductFailure(ctx context.Context, id string) bool {
	if id != "OLJCESPC7Z" {
		return false
	}

	client := openfeature.NewClient("productCatalog")
	failureEnabled, _ := client.BooleanValue(
		ctx, "productCatalogFailure", false, openfeature.EvaluationContext{},
	)
	return failureEnabled
}

func createClient(ctx context.Context, svcAddr string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, svcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
}
