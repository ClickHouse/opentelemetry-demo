// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	otelhooks "github.com/open-feature/go-sdk-contrib/hooks/open-telemetry/pkg"
	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/open-telemetry/opentelemetry-demo/src/checkout/genproto/oteldemo"
	"github.com/open-telemetry/opentelemetry-demo/src/checkout/kafka"
	"github.com/open-telemetry/opentelemetry-demo/src/checkout/money"
)

//go:generate go install google.golang.org/protobuf/cmd/protoc-gen-go
//go:generate go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
//go:generate protoc --go_out=./ --go-grpc_out=./ --proto_path=../../pb ../../pb/demo.proto

var log *logrus.Logger
var tracer trace.Tracer
var resource *sdkresource.Resource
var initResourcesOnce sync.Once

func init() {
	log = logrus.New()
	log.Level = logrus.DebugLevel
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.Out = os.Stdout
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
		log.Fatalf("new otlp trace grpc exporter failed: %v", err)
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

type checkout struct {
	productCatalogSvcAddr string
	cartSvcAddr           string
	currencySvcAddr       string
	shippingSvcAddr       string
	emailSvcAddr          string
	paymentSvcAddr        string
	kafkaBrokerSvcAddr    string
	pb.UnimplementedCheckoutServiceServer
	KafkaProducerClient     sarama.AsyncProducer
	shippingSvcClient       pb.ShippingServiceClient
	productCatalogSvcClient pb.ProductCatalogServiceClient
	cartSvcClient           pb.CartServiceClient
	currencySvcClient       pb.CurrencyServiceClient
	emailSvcClient          pb.EmailServiceClient
	paymentSvcClient        pb.PaymentServiceClient
}

func main() {
	var port string
	mustMapEnv(&port, "CHECKOUT_PORT")

	tp := initTracerProvider()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	mp := initMeterProvider()
	defer func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down meter provider: %v", err)
		}
	}()

	err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second))
	if err != nil {
		log.Fatal(err)
	}

	openfeature.SetProvider(flagd.NewProvider())
	openfeature.AddHooks(otelhooks.NewTracesHook())

	tracer = tp.Tracer("checkout")

	svc := new(checkout)

	mustMapEnv(&svc.shippingSvcAddr, "SHIPPING_ADDR")
	c := mustCreateClient(svc.shippingSvcAddr)
	svc.shippingSvcClient = pb.NewShippingServiceClient(c)
	defer c.Close()

	mustMapEnv(&svc.productCatalogSvcAddr, "PRODUCT_CATALOG_ADDR")
	c = mustCreateClient(svc.productCatalogSvcAddr)
	svc.productCatalogSvcClient = pb.NewProductCatalogServiceClient(c)
	defer c.Close()

	mustMapEnv(&svc.cartSvcAddr, "CART_ADDR")
	c = mustCreateClient(svc.cartSvcAddr)
	svc.cartSvcClient = pb.NewCartServiceClient(c)
	defer c.Close()

	mustMapEnv(&svc.currencySvcAddr, "CURRENCY_ADDR")
	c = mustCreateClient(svc.currencySvcAddr)
	svc.currencySvcClient = pb.NewCurrencyServiceClient(c)
	defer c.Close()

	mustMapEnv(&svc.emailSvcAddr, "EMAIL_ADDR")
	c = mustCreateClient(svc.emailSvcAddr)
	svc.emailSvcClient = pb.NewEmailServiceClient(c)
	defer c.Close()

	mustMapEnv(&svc.paymentSvcAddr, "PAYMENT_ADDR")
	c = mustCreateClient(svc.paymentSvcAddr)
	svc.paymentSvcClient = pb.NewPaymentServiceClient(c)
	defer c.Close()

	svc.kafkaBrokerSvcAddr = os.Getenv("KAFKA_ADDR")

	if svc.kafkaBrokerSvcAddr != "" {
		svc.KafkaProducerClient, err = kafka.CreateKafkaProducer([]string{svc.kafkaBrokerSvcAddr}, log)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Infof("service config: %+v", svc)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal(err)
	}

	var srv = grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	pb.RegisterCheckoutServiceServer(srv, svc)
	healthpb.RegisterHealthServer(srv, svc)
	log.Infof("starting to listen on tcp: %q", lis.Addr().String())
	err = srv.Serve(lis)
	log.Fatal(err)
}

func mustMapEnv(target *string, envKey string) {
	v := os.Getenv(envKey)
	if v == "" {
		panic(fmt.Sprintf("environment variable %q not set", envKey))
	}
	*target = v
}

func (cs *checkout) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (cs *checkout) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "health check via Watch not implemented")
}

// getBaggageValue extracts a baggage value from context
func getBaggageValue(ctx context.Context, key string) string {
	bag := baggage.FromContext(ctx)
	m := bag.Member(key)
	return m.Value()
}

// sha256Short returns a short sha256 hash
func sha256Short(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])[:16]
}

var (
	riskEngines           = []string{"fraud-shield-v3", "ml-scorer-v2", "rule-engine-v1", "behavioral-analysis-v1"}
	paymentGateways       = []string{"stripe", "adyen", "braintree", "square", "worldpay", "cybersource"}
	shippingCarriers      = []string{"fedex", "ups", "usps", "dhl", "amazon-logistics", "ontrac"}
	shippingMethods       = []string{"standard", "express", "next-day", "two-day", "economy", "freight"}
	warehouseLocations    = []string{"us-east-1-wh", "us-west-2-wh", "eu-west-1-wh", "ap-southeast-1-wh", "us-central-1-wh"}
	fulfillmentCenters    = []string{"FC-NYC-01", "FC-LAX-02", "FC-ORD-03", "FC-LHR-04", "FC-NRT-05", "FC-SYD-06"}
	promotionTypes        = []string{"percentage-off", "fixed-amount", "bogo", "free-shipping", "bundle-deal", "loyalty-reward", "first-order", "referral-credit"}
	taxJurisdictions      = []string{"US-NY", "US-CA", "US-TX", "US-WA", "US-FL", "GB-ENG", "DE-BY", "FR-IDF", "JP-13", "CA-ON"}
	inventoryStatuses     = []string{"in-stock", "low-stock", "backordered", "pre-order", "limited-edition"}
	checkoutFlows         = []string{"standard", "express", "one-click", "guest", "subscription", "gift"}
	pricingStrategies     = []string{"standard", "dynamic", "promotional", "clearance", "membership", "wholesale"}
	complianceRegions     = []string{"gdpr-eu", "ccpa-us", "lgpd-br", "pipa-kr", "appi-jp", "pdpa-sg"}
	retryReasons          = []string{"none", "timeout", "rate-limit", "transient-error", "network-blip"}
	authMethods           = []string{"jwt", "oauth2", "api-key", "session-cookie", "saml", "mfa-totp"}
)

func (cs *checkout) PlaceOrder(ctx context.Context, req *pb.PlaceOrderRequest) (*pb.PlaceOrderResponse, error) {
	span := trace.SpanFromContext(ctx)

	// Core business attributes
	span.SetAttributes(
		attribute.String("app.user.id", req.UserId),
		attribute.String("app.user.currency", req.UserCurrency),
	)

	// Propagated baggage attributes
	baggageKeys := []string{
		"user.tier", "user.locale", "user.country", "user.timezone",
		"user.account_age_days", "user.total_orders", "user.lifetime_value_usd",
		"user.segment", "device.type", "device.screen_resolution", "client.platform",
		"client.sdk_version", "client.browser", "client.os", "network.type",
		"traffic.referrer", "traffic.campaign_id", "traffic.channel",
		"experiment.group", "experiment.id", "feature_flags.active",
		"geo.city", "geo.region", "geo.postal_code",
		"privacy.consent_analytics", "privacy.consent_marketing",
		"request.correlation_id", "session.id", "user.anonymous_id",
		"device.id", "content.group",
	}
	for _, key := range baggageKeys {
		if v := getBaggageValue(ctx, key); v != "" {
			span.SetAttributes(attribute.String(key, v))
		}
	}

	// Rich checkout context attributes
	requestStartTime := time.Now()
	checkoutID := uuid.New().String()
	span.SetAttributes(
		attribute.String("app.checkout.id", checkoutID),
		attribute.String("app.checkout.flow", checkoutFlows[rand.Intn(len(checkoutFlows))]),
		attribute.String("app.checkout.pricing_strategy", pricingStrategies[rand.Intn(len(pricingStrategies))]),
		attribute.String("app.checkout.compliance_region", complianceRegions[rand.Intn(len(complianceRegions))]),
		attribute.String("app.checkout.auth_method", authMethods[rand.Intn(len(authMethods))]),
		attribute.Int64("app.checkout.timestamp_epoch_ms", requestStartTime.UnixMilli()),
		attribute.String("app.checkout.idempotency_key", uuid.New().String()),
		attribute.String("app.checkout.request_fingerprint", sha256Short(req.UserId+checkoutID)),
		attribute.Bool("app.checkout.is_retry", rand.Float64() < 0.05),
		attribute.String("app.checkout.retry_reason", retryReasons[rand.Intn(len(retryReasons))]),
		attribute.Int("app.checkout.retry_count", rand.Intn(3)),

		// Risk and fraud attributes
		attribute.Float64("app.risk.score", rand.Float64()*100),
		attribute.String("app.risk.engine", riskEngines[rand.Intn(len(riskEngines))]),
		attribute.String("app.risk.decision", []string{"approve", "approve", "approve", "review", "decline"}[rand.Intn(5)]),
		attribute.Int("app.risk.evaluation_time_ms", rand.Intn(150)+10),
		attribute.Bool("app.risk.velocity_check_passed", rand.Float64() > 0.02),
		attribute.Int("app.risk.signals_evaluated", rand.Intn(25)+5),
		attribute.String("app.risk.model_version", fmt.Sprintf("v%d.%d.%d", rand.Intn(3)+1, rand.Intn(10), rand.Intn(20))),

		// Shipping attributes
		attribute.String("app.shipping.carrier", shippingCarriers[rand.Intn(len(shippingCarriers))]),
		attribute.String("app.shipping.method", shippingMethods[rand.Intn(len(shippingMethods))]),
		attribute.Int("app.shipping.estimated_days", rand.Intn(14)+1),
		attribute.String("app.shipping.warehouse", warehouseLocations[rand.Intn(len(warehouseLocations))]),
		attribute.String("app.shipping.fulfillment_center", fulfillmentCenters[rand.Intn(len(fulfillmentCenters))]),
		attribute.Bool("app.shipping.signature_required", rand.Float64() < 0.2),
		attribute.Bool("app.shipping.insurance_added", rand.Float64() < 0.15),
		attribute.Float64("app.shipping.package_weight_kg", rand.Float64()*20+0.1),
		attribute.String("app.shipping.package_dimensions", fmt.Sprintf("%dx%dx%d", rand.Intn(60)+10, rand.Intn(40)+10, rand.Intn(30)+5)),
		attribute.String("app.shipping.destination.country", req.Address.GetCountry()),
		attribute.String("app.shipping.destination.state", req.Address.GetState()),
		attribute.String("app.shipping.destination.city", req.Address.GetCity()),
		attribute.String("app.shipping.destination.zip", req.Address.GetZipCode()),

		// Payment preprocessing attributes
		attribute.String("app.payment.gateway", paymentGateways[rand.Intn(len(paymentGateways))]),
		attribute.String("app.payment.method_type", []string{"credit-card", "credit-card", "credit-card", "debit-card", "digital-wallet"}[rand.Intn(5)]),
		attribute.Bool("app.payment.3ds_required", rand.Float64() < 0.3),
		attribute.String("app.payment.billing_country", req.Address.GetCountry()),

		// Promotion/discount attributes
		attribute.Bool("app.promotion.applied", rand.Float64() < 0.35),
		attribute.String("app.promotion.code", fmt.Sprintf("PROMO%d", rand.Intn(1000))),
		attribute.String("app.promotion.type", promotionTypes[rand.Intn(len(promotionTypes))]),
		attribute.Float64("app.promotion.discount_amount", rand.Float64()*50),
		attribute.Float64("app.promotion.discount_percent", float64(rand.Intn(50))),

		// Tax attributes
		attribute.String("app.tax.jurisdiction", taxJurisdictions[rand.Intn(len(taxJurisdictions))]),
		attribute.Float64("app.tax.rate", float64(rand.Intn(25))/100.0),
		attribute.Bool("app.tax.exempt", rand.Float64() < 0.05),
		attribute.String("app.tax.calculation_method", []string{"inclusive", "exclusive"}[rand.Intn(2)]),

		// Inventory attributes
		attribute.String("app.inventory.status", inventoryStatuses[rand.Intn(len(inventoryStatuses))]),
		attribute.Int("app.inventory.reserved_units", rand.Intn(10)+1),
		attribute.String("app.inventory.reservation_id", uuid.New().String()),
		attribute.Int64("app.inventory.reservation_ttl_sec", int64(rand.Intn(300)+60)),

		// Infrastructure attributes
		attribute.String("app.infra.handler_instance", fmt.Sprintf("checkout-%d", rand.Intn(10))),
		attribute.Int("app.infra.goroutine_count", rand.Intn(500)+50),
		attribute.Int64("app.infra.memory_alloc_bytes", int64(rand.Intn(500000000)+10000000)),
		attribute.String("app.infra.deployment_id", fmt.Sprintf("deploy-%s", uuid.New().String()[:8])),
		attribute.String("app.infra.canary_group", []string{"stable", "stable", "canary"}[rand.Intn(3)]),
		attribute.String("app.infra.load_balancer_id", fmt.Sprintf("lb-%s", uuid.New().String()[:8])),
		attribute.Int("app.infra.upstream_latency_ms", rand.Intn(200)+5),
		attribute.Bool("app.infra.circuit_breaker_open", rand.Float64() < 0.01),
		attribute.String("app.infra.rate_limit_bucket", fmt.Sprintf("checkout-%s", req.UserCurrency)),
		attribute.Int("app.infra.rate_limit_remaining", rand.Intn(1000)+100),
	)

	log.WithFields(logrus.Fields{
		"app.user.id":                  req.UserId,
		"app.user.currency":            req.UserCurrency,
		"app.checkout.id":              checkoutID,
		"app.checkout.flow":            checkoutFlows[rand.Intn(len(checkoutFlows))],
		"app.request.correlation_id":   uuid.New().String(),
		"app.request.idempotency_key":  uuid.New().String(),
		"app.request.timestamp_ms":     requestStartTime.UnixMilli(),
		"app.request.priority":         []string{"critical", "high", "normal", "low"}[rand.Intn(4)],
		"app.session.id":               getBaggageValue(ctx, "session.id"),
		"app.user.tier":                getBaggageValue(ctx, "user.tier"),
		"app.user.segment":             getBaggageValue(ctx, "user.segment"),
		"app.user.country":             getBaggageValue(ctx, "user.country"),
		"app.geo.city":                 getBaggageValue(ctx, "geo.city"),
		"app.device.type":              getBaggageValue(ctx, "device.type"),
		"app.client.platform":          getBaggageValue(ctx, "client.platform"),
		"app.traffic.channel":          getBaggageValue(ctx, "traffic.channel"),
		"app.traffic.campaign_id":      getBaggageValue(ctx, "traffic.campaign_id"),
		"app.experiment.id":            getBaggageValue(ctx, "experiment.id"),
		"app.experiment.group":         getBaggageValue(ctx, "experiment.group"),
		"net.peer.address":             fmt.Sprintf("10.0.%d.%d", rand.Intn(256), rand.Intn(254)+1),
		"net.host.port":                5050,
		"http.request_content_length":  rand.Intn(5000) + 100,
		"infra.handler_instance":       fmt.Sprintf("checkout-%d", rand.Intn(10)),
		"infra.goroutine_count":        rand.Intn(500) + 50,
		"infra.memory_alloc_mb":        rand.Intn(500) + 50,
		"infra.gc_pause_ms":            rand.Intn(50),
		"infra.upstream_latency_ms":    rand.Intn(200) + 5,
		"infra.circuit_breaker_state":  []string{"closed", "closed", "closed", "half-open", "open"}[rand.Intn(5)],
		"infra.rate_limit_remaining":   rand.Intn(1000) + 100,
		"infra.connection_pool_active": rand.Intn(50) + 1,
		"infra.deployment_id":          fmt.Sprintf("deploy-%s", uuid.New().String()[:8]),
	}).Infof("[PlaceOrder] user_id=%q user_currency=%q", req.UserId, req.UserCurrency)

	var err error
	defer func() {
		if err != nil {
			span.AddEvent("error", trace.WithAttributes(semconv.ExceptionMessageKey.String(err.Error())))
		}
	}()

	orderID, err := uuid.NewUUID()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to generate order uuid")
	}

	prep, err := cs.prepareOrderItemsAndShippingQuoteFromCart(ctx, req.UserId, req.UserCurrency, req.Address)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	span.AddEvent("prepared")

	total := &pb.Money{CurrencyCode: req.UserCurrency,
		Units: 0,
		Nanos: 0}
	total = money.Must(money.Sum(total, prep.shippingCostLocalized))
	for _, it := range prep.orderItems {
		multPrice := money.MultiplySlow(it.Cost, uint32(it.GetItem().GetQuantity()))
		total = money.Must(money.Sum(total, multPrice))
	}

	txID, err := cs.chargeCard(ctx, total, req.CreditCard)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to charge card: %+v", err)
	}
	log.WithFields(logrus.Fields{
		"app.payment.transaction_id":  txID,
		"app.payment.gateway":         paymentGateways[rand.Intn(len(paymentGateways))],
		"app.payment.amount_units":    total.GetUnits(),
		"app.payment.amount_nanos":    total.GetNanos(),
		"app.payment.currency":        req.UserCurrency,
		"app.payment.method":          []string{"credit-card", "credit-card", "debit-card", "digital-wallet"}[rand.Intn(4)],
		"app.payment.processing_ms":   rand.Intn(1500) + 100,
		"app.payment.risk_score":      rand.Float64() * 100,
		"app.payment.3ds_used":        rand.Float64() < 0.3,
		"app.checkout.id":             checkoutID,
		"app.order.user_id":           req.UserId,
	}).Infof("payment went through (transaction_id: %s)", txID)
	span.AddEvent("charged",
		trace.WithAttributes(attribute.String("app.payment.transaction.id", txID)))

	shippingTrackingID, err := cs.shipOrder(ctx, req.Address, prep.cartItems)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "shipping error: %+v", err)
	}
	shippingTrackingAttribute := attribute.String("app.shipping.tracking.id", shippingTrackingID)
	span.AddEvent("shipped", trace.WithAttributes(shippingTrackingAttribute))

	_ = cs.emptyUserCart(ctx, req.UserId)

	orderResult := &pb.OrderResult{
		OrderId:            orderID.String(),
		ShippingTrackingId: shippingTrackingID,
		ShippingCost:       prep.shippingCostLocalized,
		ShippingAddress:    req.Address,
		Items:              prep.orderItems,
	}

	shippingCostFloat, _ := strconv.ParseFloat(fmt.Sprintf("%d.%02d", prep.shippingCostLocalized.GetUnits(), prep.shippingCostLocalized.GetNanos()/1000000000), 64)
	totalPriceFloat, _ := strconv.ParseFloat(fmt.Sprintf("%d.%02d", total.GetUnits(), total.GetNanos()/1000000000), 64)

	span.SetAttributes(
		attribute.String("app.order.id", orderID.String()),
		attribute.Float64("app.shipping.amount", shippingCostFloat),
		attribute.Float64("app.order.amount", totalPriceFloat),
		attribute.Int("app.order.items.count", len(prep.orderItems)),
		shippingTrackingAttribute,
		// Additional order completion attributes
		attribute.Float64("app.order.subtotal", totalPriceFloat-shippingCostFloat),
		attribute.String("app.order.currency", req.UserCurrency),
		attribute.String("app.order.status", "confirmed"),
		attribute.Int64("app.order.completed_at_epoch_ms", time.Now().UnixMilli()),
		attribute.Int("app.order.processing_time_ms", int(time.Since(requestStartTime).Milliseconds())),
		attribute.String("app.order.confirmation_email", req.Email),
		attribute.String("app.order.user_agent_hash", sha256Short(req.UserId)),
		attribute.Bool("app.order.is_gift", rand.Float64() < 0.1),
		attribute.Bool("app.order.requires_age_verification", rand.Float64() < 0.05),
		attribute.String("app.order.fraud_check_result", "pass"),
		attribute.Int("app.order.loyalty_points_earned", rand.Intn(500)+10),
		attribute.String("app.order.estimated_delivery", time.Now().AddDate(0, 0, rand.Intn(14)+1).Format("2006-01-02")),
	)

	if err := cs.sendOrderConfirmation(ctx, req.Email, orderResult); err != nil {
		log.WithFields(logrus.Fields{
			"app.email.recipient":     req.Email,
			"app.email.type":          "order-confirmation",
			"app.email.error":         err.Error(),
			"app.checkout.id":         checkoutID,
			"app.order.id":            orderID.String(),
			"app.email.retry_count":   0,
			"app.email.template":      "order-confirmation-v3",
			"app.email.provider":      []string{"sendgrid", "ses", "mailgun", "postmark"}[rand.Intn(4)],
		}).Warnf("failed to send order confirmation to %q: %+v", req.Email, err)
	} else {
		log.WithFields(logrus.Fields{
			"app.email.recipient":     req.Email,
			"app.email.type":          "order-confirmation",
			"app.email.status":        "sent",
			"app.checkout.id":         checkoutID,
			"app.order.id":            orderID.String(),
			"app.email.template":      "order-confirmation-v3",
			"app.email.provider":      []string{"sendgrid", "ses", "mailgun", "postmark"}[rand.Intn(4)],
			"app.email.delivery_ms":   rand.Intn(2000) + 50,
		}).Infof("order confirmation email sent to %q", req.Email)
	}

	// send to kafka only if kafka broker address is set
	if cs.kafkaBrokerSvcAddr != "" {
		log.WithFields(logrus.Fields{
			"app.kafka.topic":           "orders",
			"app.kafka.broker":          svc.kafkaBrokerSvcAddr,
			"app.order.id":              orderResult.OrderId,
			"app.checkout.id":           checkoutID,
			"app.kafka.partition":       rand.Intn(12),
			"app.kafka.message_size_bytes": rand.Intn(5000) + 200,
		}).Infof("sending to postProcessor")
		cs.sendToPostProcessor(ctx, orderResult)
	}

	resp := &pb.PlaceOrderResponse{Order: orderResult}
	return resp, nil
}

type orderPrep struct {
	orderItems            []*pb.OrderItem
	cartItems             []*pb.CartItem
	shippingCostLocalized *pb.Money
}

func (cs *checkout) prepareOrderItemsAndShippingQuoteFromCart(ctx context.Context, userID, userCurrency string, address *pb.Address) (orderPrep, error) {

	ctx, span := tracer.Start(ctx, "prepareOrderItemsAndShippingQuoteFromCart")
	defer span.End()

	var out orderPrep
	cartItems, err := cs.getUserCart(ctx, userID)
	if err != nil {
		return out, fmt.Errorf("cart failure: %+v", err)
	}
	orderItems, err := cs.prepOrderItems(ctx, cartItems, userCurrency)
	if err != nil {
		return out, fmt.Errorf("failed to prepare order: %+v", err)
	}
	shippingUSD, err := cs.quoteShipping(ctx, address, cartItems)
	if err != nil {
		return out, fmt.Errorf("shipping quote failure: %+v", err)
	}
	shippingPrice, err := cs.convertCurrency(ctx, shippingUSD, userCurrency)
	if err != nil {
		return out, fmt.Errorf("failed to convert shipping cost to currency: %+v", err)
	}

	out.shippingCostLocalized = shippingPrice
	out.cartItems = cartItems
	out.orderItems = orderItems

	var totalCart int32
	for _, ci := range cartItems {
		totalCart += ci.Quantity
	}
	shippingCostFloat, _ := strconv.ParseFloat(fmt.Sprintf("%d.%02d", shippingPrice.GetUnits(), shippingPrice.GetNanos()/1000000000), 64)

	span.SetAttributes(
		attribute.Float64("app.shipping.amount", shippingCostFloat),
		attribute.Int("app.cart.items.count", int(totalCart)),
		attribute.Int("app.order.items.count", len(orderItems)),
		// Cart analysis attributes
		attribute.Int("app.cart.unique_items", len(cartItems)),
		attribute.Int("app.cart.total_quantity", int(totalCart)),
		attribute.Float64("app.cart.average_item_price", shippingCostFloat/float64(max(int(totalCart), 1))),
		attribute.Bool("app.cart.has_heavy_items", rand.Float64() < 0.2),
		attribute.Bool("app.cart.has_hazmat", rand.Float64() < 0.02),
		attribute.Bool("app.cart.has_oversized", rand.Float64() < 0.05),
		attribute.String("app.cart.primary_category", []string{"binoculars", "telescopes", "accessories", "assembly", "travel", "books"}[rand.Intn(6)]),
		attribute.Int("app.cart.abandoned_count_prior", rand.Intn(5)),
		attribute.String("app.cart.session_duration", fmt.Sprintf("%ds", rand.Intn(1800)+30)),
		attribute.Int("app.cart.items_added_count", rand.Intn(10)+int(totalCart)),
		attribute.Int("app.cart.items_removed_count", rand.Intn(3)),
		attribute.Float64("app.shipping.quote_amount", shippingCostFloat),
		attribute.String("app.shipping.quote_currency", userCurrency),
		attribute.Int("app.shipping.quote_calculation_ms", rand.Intn(50)+5),
	)
	return out, nil
}

func mustCreateClient(svcAddr string) *grpc.ClientConn {
	c, err := grpc.NewClient(svcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalf("could not connect to %s service, err: %+v", svcAddr, err)
	}

	return c
}

func (cs *checkout) quoteShipping(ctx context.Context, address *pb.Address, items []*pb.CartItem) (*pb.Money, error) {
	shippingQuote, err := cs.shippingSvcClient.
		GetQuote(ctx, &pb.GetQuoteRequest{
			Address: address,
			Items:   items})
	if err != nil {
		return nil, fmt.Errorf("failed to get shipping quote: %+v", err)
	}
	return shippingQuote.GetCostUsd(), nil
}

func (cs *checkout) getUserCart(ctx context.Context, userID string) ([]*pb.CartItem, error) {
	cart, err := cs.cartSvcClient.GetCart(ctx, &pb.GetCartRequest{UserId: userID})
	if err != nil {
		return nil, fmt.Errorf("failed to get user cart during checkout: %+v", err)
	}
	return cart.GetItems(), nil
}

func (cs *checkout) emptyUserCart(ctx context.Context, userID string) error {
	if _, err := cs.cartSvcClient.EmptyCart(ctx, &pb.EmptyCartRequest{UserId: userID}); err != nil {
		return fmt.Errorf("failed to empty user cart during checkout: %+v", err)
	}
	return nil
}

func (cs *checkout) prepOrderItems(ctx context.Context, items []*pb.CartItem, userCurrency string) ([]*pb.OrderItem, error) {
	out := make([]*pb.OrderItem, len(items))

	for i, item := range items {
		product, err := cs.productCatalogSvcClient.GetProduct(ctx, &pb.GetProductRequest{Id: item.GetProductId()})
		if err != nil {
			return nil, fmt.Errorf("failed to get product #%q", item.GetProductId())
		}
		price, err := cs.convertCurrency(ctx, product.GetPriceUsd(), userCurrency)
		if err != nil {
			return nil, fmt.Errorf("failed to convert price of %q to %s", item.GetProductId(), userCurrency)
		}
		out[i] = &pb.OrderItem{
			Item: item,
			Cost: price}
	}
	return out, nil
}

func (cs *checkout) convertCurrency(ctx context.Context, from *pb.Money, toCurrency string) (*pb.Money, error) {
	result, err := cs.currencySvcClient.Convert(ctx, &pb.CurrencyConversionRequest{
		From:   from,
		ToCode: toCurrency})
	if err != nil {
		return nil, fmt.Errorf("failed to convert currency: %+v", err)
	}
	return result, err
}

func (cs *checkout) chargeCard(ctx context.Context, amount *pb.Money, paymentInfo *pb.CreditCardInfo) (string, error) {
	paymentService := cs.paymentSvcClient
	if cs.isFeatureFlagEnabled(ctx, "paymentUnreachable") {
		badAddress := "badAddress:50051"
		c := mustCreateClient(badAddress)
		paymentService = pb.NewPaymentServiceClient(c)
	}

	paymentResp, err := paymentService.Charge(ctx, &pb.ChargeRequest{
		Amount:     amount,
		CreditCard: paymentInfo})
	if err != nil {
		return "", fmt.Errorf("could not charge the card: %+v", err)
	}
	return paymentResp.GetTransactionId(), nil
}

func (cs *checkout) sendOrderConfirmation(ctx context.Context, email string, order *pb.OrderResult) error {
	emailPayload, err := json.Marshal(map[string]interface{}{
		"email": email,
		"order": order,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal order to JSON: %+v", err)
	}

	resp, err := otelhttp.Post(ctx, cs.emailSvcAddr+"/send_order_confirmation", "application/json", bytes.NewBuffer(emailPayload))
	if err != nil {
		return fmt.Errorf("failed POST to email service: %+v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed POST to email service: expected 200, got %d", resp.StatusCode)
	}

	return err
}

func (cs *checkout) shipOrder(ctx context.Context, address *pb.Address, items []*pb.CartItem) (string, error) {
	resp, err := cs.shippingSvcClient.ShipOrder(ctx, &pb.ShipOrderRequest{
		Address: address,
		Items:   items})
	if err != nil {
		return "", fmt.Errorf("shipment failed: %+v", err)
	}
	return resp.GetTrackingId(), nil
}

func (cs *checkout) sendToPostProcessor(ctx context.Context, result *pb.OrderResult) {
	message, err := proto.Marshal(result)
	if err != nil {
		log.Errorf("Failed to marshal message to protobuf: %+v", err)
		return
	}

	msg := sarama.ProducerMessage{
		Topic: kafka.Topic,
		Value: sarama.ByteEncoder(message),
	}

	// Inject tracing info into message
	span := createProducerSpan(ctx, &msg)
	defer span.End()

	// Send message and handle response
	startTime := time.Now()
	select {
	case cs.KafkaProducerClient.Input() <- &msg:
		log.Infof("Message sent to Kafka: %v", msg)
		select {
		case successMsg := <-cs.KafkaProducerClient.Successes():
			span.SetAttributes(
				attribute.Bool("messaging.kafka.producer.success", true),
				attribute.Int("messaging.kafka.producer.duration_ms", int(time.Since(startTime).Milliseconds())),
				attribute.KeyValue(semconv.MessagingKafkaMessageOffset(int(successMsg.Offset))),
			)
			log.WithFields(logrus.Fields{
				"kafka.offset":             successMsg.Offset,
				"kafka.duration_ms":        time.Since(startTime).Milliseconds(),
				"kafka.topic":              kafka.Topic,
				"kafka.partition":          successMsg.Partition,
				"kafka.broker_id":          rand.Intn(5),
				"kafka.ack_latency_ms":     rand.Intn(50) + 1,
				"kafka.batch_size":         rand.Intn(10) + 1,
				"kafka.compression":        []string{"snappy", "gzip", "lz4", "zstd", "none"}[rand.Intn(5)],
				"kafka.message_size_bytes": rand.Intn(5000) + 200,
			}).Infof("Successful to write message. offset: %v, duration: %v", successMsg.Offset, time.Since(startTime))
		case errMsg := <-cs.KafkaProducerClient.Errors():
			span.SetAttributes(
				attribute.Bool("messaging.kafka.producer.success", false),
				attribute.Int("messaging.kafka.producer.duration_ms", int(time.Since(startTime).Milliseconds())),
			)
			span.SetStatus(otelcodes.Error, errMsg.Err.Error())
			log.WithFields(logrus.Fields{
				"kafka.error":         errMsg.Err.Error(),
				"kafka.topic":         kafka.Topic,
				"kafka.duration_ms":   time.Since(startTime).Milliseconds(),
				"kafka.retry_count":   rand.Intn(3),
				"kafka.error_type":    []string{"network", "timeout", "leader-not-available", "broker-unavailable"}[rand.Intn(4)],
			}).Errorf("Failed to write message: %v", errMsg.Err)
		case <-ctx.Done():
			span.SetAttributes(
				attribute.Bool("messaging.kafka.producer.success", false),
				attribute.Int("messaging.kafka.producer.duration_ms", int(time.Since(startTime).Milliseconds())),
			)
			span.SetStatus(otelcodes.Error, "Context cancelled: "+ctx.Err().Error())
			log.Warnf("Context canceled before success message received: %v", ctx.Err())
		}
	case <-ctx.Done():
		span.SetAttributes(
			attribute.Bool("messaging.kafka.producer.success", false),
			attribute.Int("messaging.kafka.producer.duration_ms", int(time.Since(startTime).Milliseconds())),
		)
		span.SetStatus(otelcodes.Error, "Failed to send: "+ctx.Err().Error())
		log.Errorf("Failed to send message to Kafka within context deadline: %v", ctx.Err())
		return
	}

	ffValue := cs.getIntFeatureFlag(ctx, "kafkaQueueProblems")
	if ffValue > 0 {
		log.Infof("Warning: FeatureFlag 'kafkaQueueProblems' is activated, overloading queue now.")
		for i := 0; i < ffValue; i++ {
			go func(i int) {
				cs.KafkaProducerClient.Input() <- &msg
				_ = <-cs.KafkaProducerClient.Successes()
			}(i)
		}
		log.Infof("Done with #%d messages for overload simulation.", ffValue)
	}
}

func createProducerSpan(ctx context.Context, msg *sarama.ProducerMessage) trace.Span {
	spanContext, span := tracer.Start(
		ctx,
		fmt.Sprintf("%s publish", msg.Topic),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.PeerService("kafka"),
			semconv.NetworkTransportTCP,
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingOperationPublish,
			semconv.MessagingKafkaDestinationPartition(int(msg.Partition)),
		),
	)

	carrier := propagation.MapCarrier{}
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(spanContext, carrier)

	for key, value := range carrier {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(key), Value: []byte(value)})
	}

	return span
}

func (cs *checkout) isFeatureFlagEnabled(ctx context.Context, featureFlagName string) bool {
	client := openfeature.NewClient("checkout")

	// Default value is set to false, but you could also make this a parameter.
	featureEnabled, _ := client.BooleanValue(
		ctx,
		featureFlagName,
		false,
		openfeature.EvaluationContext{},
	)

	return featureEnabled
}

func (cs *checkout) getIntFeatureFlag(ctx context.Context, featureFlagName string) int {
	client := openfeature.NewClient("checkout")

	// Default value is set to 0, but you could also make this a parameter.
	featureFlagValue, _ := client.IntValue(
		ctx,
		featureFlagName,
		0,
		openfeature.EvaluationContext{},
	)

	return int(featureFlagValue)
}
