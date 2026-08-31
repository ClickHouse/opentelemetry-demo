/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package oteldemo;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import io.grpc.*;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.*;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import oteldemo.Demo.Ad;
import oteldemo.Demo.AdRequest;
import oteldemo.Demo.AdResponse;
import oteldemo.problempattern.GarbageCollectionTrigger;
import oteldemo.problempattern.CPULoad;
import dev.openfeature.contrib.providers.flagd.FlagdOptions;
import dev.openfeature.contrib.providers.flagd.FlagdProvider;
import dev.openfeature.sdk.Client;
import dev.openfeature.sdk.EvaluationContext;
import dev.openfeature.sdk.MutableContext;
import dev.openfeature.sdk.OpenFeatureAPI;
import java.util.UUID;


public final class AdService {

  private static final Logger logger = LogManager.getLogger(AdService.class);

  @SuppressWarnings("FieldCanBeLocal")
  private static final int MAX_ADS_TO_SERVE = 2;

  private Server server;
  private HealthStatusManager healthMgr;

  private static final AdService service = new AdService();
  private static final Tracer tracer = GlobalOpenTelemetry.getTracer("ad");
  private static final Meter meter = GlobalOpenTelemetry.getMeter("ad");

  private static final LongCounter adRequestsCounter =
      meter
          .counterBuilder("app.ads.ad_requests")
          .setDescription("Counts ad requests by request and response type")
          .build();

  private static final AttributeKey<String> adRequestTypeKey =
      AttributeKey.stringKey("app.ads.ad_request_type");
  private static final AttributeKey<String> adResponseTypeKey =
      AttributeKey.stringKey("app.ads.ad_response_type");

  private void start() throws IOException {
    int port =
        Integer.parseInt(
            Optional.ofNullable(System.getenv("AD_PORT"))
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "environment vars: AD_PORT must not be null")));
    healthMgr = new HealthStatusManager();

    // Create a flagd instance with OpenTelemetry
    FlagdOptions options =
        FlagdOptions.builder()
            .withGlobalTelemetry(true)
            .build();

    FlagdProvider flagdProvider = new FlagdProvider(options);
    // Set flagd as the OpenFeature Provider
    OpenFeatureAPI.getInstance().setProvider(flagdProvider);
  
    server =
        ServerBuilder.forPort(port)
            .addService(new AdServiceImpl())
            .addService(healthMgr.getHealthService())
            .build()
            .start();
    logger.info("Ad service started, listening on " + port);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                  System.err.println(
                      "*** shutting down gRPC ads server since JVM is shutting down");
                  AdService.this.stop();
                  System.err.println("*** server shut down");
                }));
    healthMgr.setStatus("", ServingStatus.SERVING);
  }

  private void stop() {
    if (server != null) {
      healthMgr.clearStatus("");
      server.shutdown();
    }
  }

  private enum AdRequestType {
    TARGETED,
    NOT_TARGETED
  }

  private enum AdResponseType {
    TARGETED,
    RANDOM
  }

  // Rich attribute value pools for realistic ad serving
  private static final String[] AD_PLACEMENTS = {"header-banner", "sidebar", "inline-content", "footer", "interstitial", "native-feed", "search-results", "product-page", "cart-page", "checkout"};
  private static final String[] AD_FORMATS = {"banner-728x90", "banner-300x250", "banner-160x600", "native", "video-pre-roll", "video-mid-roll", "carousel", "rich-media", "text-only", "shopping"};
  private static final String[] AD_EXCHANGES = {"google-ads", "facebook-audience", "amazon-dsp", "trade-desk", "criteo", "taboola", "outbrain", "direct-sold"};
  private static final String[] AD_CAMPAIGNS = {"spring-awareness-2024", "retargeting-high-value", "new-product-launch", "seasonal-clearance", "brand-lift-study", "competitor-conquest", "loyalty-upsell", "abandoned-cart-recovery"};
  private static final String[] ADVERTISER_VERTICALS = {"astronomy-equipment", "outdoor-recreation", "photography", "education", "travel", "technology", "books-media"};
  private static final String[] CREATIVE_VERSIONS = {"v1-original", "v2-refreshed", "v3-holiday", "v4-dynamic", "v5-personalized"};
  private static final String[] TARGETING_METHODS = {"contextual", "behavioral", "demographic", "geographic", "retargeting", "lookalike", "interest-based", "keyword"};
  private static final String[] BID_STRATEGIES = {"cpc-manual", "cpc-enhanced", "cpm-target", "cpa-target", "roas-target", "viewable-cpm", "maximize-clicks", "maximize-conversions"};
  private static final String[] AUDIENCE_SEGMENTS = {"in-market-astronomy", "affinity-outdoor", "custom-intent-telescopes", "remarketing-30d", "similar-to-purchasers", "new-visitors", "cart-abandoners"};
  private static final String[] AD_QUALITY_TIERS = {"premium", "standard", "remnant", "programmatic-guaranteed"};
  private static final String[] VIEWABILITY_PREDICTIONS = {"high", "medium", "low"};
  private static final String[] BRAND_SAFETY_LEVELS = {"strict", "standard", "relaxed"};
  private static final String[] RENDERING_ENGINES = {"server-side", "client-side-js", "amp", "prebid"};

  private static class AdServiceImpl extends oteldemo.AdServiceGrpc.AdServiceImplBase {

    private static final String AD_FAILURE = "adFailure";
    private static final String AD_MANUAL_GC_FEATURE_FLAG = "adManualGc";
    private static final String AD_HIGH_CPU_FEATURE_FLAG = "adHighCpu";
    private static final Client ffClient = OpenFeatureAPI.getInstance().getClient();

    private AdServiceImpl() {}

    /**
     * Retrieves ads based on context provided in the request {@code AdRequest}.
     *
     * @param req the request containing context.
     * @param responseObserver the stream observer which gets notified with the value of {@code
     *     AdResponse}
     */
    @Override
    public void getAds(AdRequest req, StreamObserver<AdResponse> responseObserver) {
      AdService service = AdService.getInstance();

      // get the current span in context
      Span span = Span.current();
      try {
        List<Ad> allAds = new ArrayList<>();
        AdRequestType adRequestType;
        AdResponseType adResponseType;

        Baggage baggage = Baggage.fromContextOrNull(Context.current());
        MutableContext evaluationContext = new MutableContext();
        if (baggage != null) {
          final String sessionId = baggage.getEntryValue("session.id");
          span.setAttribute("session.id", sessionId);
          evaluationContext.setTargetingKey(sessionId);
          evaluationContext.add("session", sessionId);
        } else {
          logger.info("no baggage found in context");
        }

        CPULoad cpuload = CPULoad.getInstance();
        cpuload.execute(ffClient.getBooleanValue(AD_HIGH_CPU_FEATURE_FLAG, false, evaluationContext));

        span.setAttribute("app.ads.contextKeys", req.getContextKeysList().toString());
        span.setAttribute("app.ads.contextKeys.count", req.getContextKeysCount());
        if (req.getContextKeysCount() > 0) {
          // Set rich structured log context via MDC
          ThreadLocalRandom tlr2 = ThreadLocalRandom.current();
          ThreadContext.put("ad.operation", "getAds");
          ThreadContext.put("ad.request_id", UUID.randomUUID().toString());
          ThreadContext.put("ad.request_type", "targeted");
          ThreadContext.put("ad.context_keys", req.getContextKeysList().toString());
          ThreadContext.put("ad.context_keys_count", String.valueOf(req.getContextKeysCount()));
          ThreadContext.put("ad.campaign_id", AD_CAMPAIGNS[tlr2.nextInt(AD_CAMPAIGNS.length)]);
          ThreadContext.put("ad.exchange", AD_EXCHANGES[tlr2.nextInt(AD_EXCHANGES.length)]);
          ThreadContext.put("ad.placement", AD_PLACEMENTS[tlr2.nextInt(AD_PLACEMENTS.length)]);
          ThreadContext.put("ad.targeting_method", TARGETING_METHODS[tlr2.nextInt(TARGETING_METHODS.length)]);
          ThreadContext.put("ad.audience_segment", AUDIENCE_SEGMENTS[tlr2.nextInt(AUDIENCE_SEGMENTS.length)]);
          ThreadContext.put("ad.bid_strategy", BID_STRATEGIES[tlr2.nextInt(BID_STRATEGIES.length)]);
          ThreadContext.put("ad.quality_tier", AD_QUALITY_TIERS[tlr2.nextInt(AD_QUALITY_TIERS.length)]);
          ThreadContext.put("ad.predicted_ctr", String.valueOf(Math.round(tlr2.nextDouble(0.001, 0.15) * 10000.0) / 10000.0));
          ThreadContext.put("ad.auction_participants", String.valueOf(tlr2.nextInt(2, 30)));
          ThreadContext.put("ad.bid_amount_usd", String.valueOf(Math.round(tlr2.nextDouble(0.01, 15.0) * 1000.0) / 1000.0));
          ThreadContext.put("ad.brand_safety_level", BRAND_SAFETY_LEVELS[tlr2.nextInt(BRAND_SAFETY_LEVELS.length)]);
          ThreadContext.put("ad.frequency_cap_remaining", String.valueOf(tlr2.nextInt(0, 20)));
          ThreadContext.put("ad.rendering_engine", RENDERING_ENGINES[tlr2.nextInt(RENDERING_ENGINES.length)]);
          ThreadContext.put("ad.creative_format", AD_FORMATS[tlr2.nextInt(AD_FORMATS.length)]);
          ThreadContext.put("ad.ml_model_version", "ad-rank-v" + tlr2.nextInt(1, 6) + "." + tlr2.nextInt(0, 20));
          ThreadContext.put("infra.handler_instance", "ad-service-" + tlr2.nextInt(0, 8));
          ThreadContext.put("infra.heap_used_mb", String.valueOf(tlr2.nextInt(128, 2048)));
          ThreadContext.put("infra.thread_pool_active", String.valueOf(tlr2.nextInt(1, 50)));
          ThreadContext.put("infra.cache_hit_rate", String.valueOf(Math.round(tlr2.nextDouble(0.5, 0.99) * 1000.0) / 1000.0));
          ThreadContext.put("net.peer_address", "10.0." + tlr2.nextInt(0, 256) + "." + tlr2.nextInt(1, 255));
          ThreadContext.put("grpc.method", "/oteldemo.AdService/GetAds");
          ThreadContext.put("grpc.status_code", "OK");
          logger.info("Targeted ad request received for " + req.getContextKeysList());
          for (int i = 0; i < req.getContextKeysCount(); i++) {
            Collection<Ad> ads = service.getAdsByCategory(req.getContextKeys(i));
            allAds.addAll(ads);
          }
          adRequestType = AdRequestType.TARGETED;
          adResponseType = AdResponseType.TARGETED;
        } else {
          ThreadLocalRandom tlr2 = ThreadLocalRandom.current();
          ThreadContext.put("ad.operation", "getAds");
          ThreadContext.put("ad.request_id", UUID.randomUUID().toString());
          ThreadContext.put("ad.request_type", "non-targeted");
          ThreadContext.put("ad.context_keys_count", "0");
          ThreadContext.put("ad.campaign_id", AD_CAMPAIGNS[tlr2.nextInt(AD_CAMPAIGNS.length)]);
          ThreadContext.put("ad.exchange", AD_EXCHANGES[tlr2.nextInt(AD_EXCHANGES.length)]);
          ThreadContext.put("ad.placement", AD_PLACEMENTS[tlr2.nextInt(AD_PLACEMENTS.length)]);
          ThreadContext.put("ad.targeting_method", "random");
          ThreadContext.put("ad.quality_tier", AD_QUALITY_TIERS[tlr2.nextInt(AD_QUALITY_TIERS.length)]);
          ThreadContext.put("ad.auction_participants", String.valueOf(tlr2.nextInt(2, 30)));
          ThreadContext.put("ad.creative_format", AD_FORMATS[tlr2.nextInt(AD_FORMATS.length)]);
          ThreadContext.put("ad.ml_model_version", "ad-rank-v" + tlr2.nextInt(1, 6) + "." + tlr2.nextInt(0, 20));
          ThreadContext.put("infra.handler_instance", "ad-service-" + tlr2.nextInt(0, 8));
          ThreadContext.put("infra.heap_used_mb", String.valueOf(tlr2.nextInt(128, 2048)));
          ThreadContext.put("infra.thread_pool_active", String.valueOf(tlr2.nextInt(1, 50)));
          ThreadContext.put("net.peer_address", "10.0." + tlr2.nextInt(0, 256) + "." + tlr2.nextInt(1, 255));
          ThreadContext.put("grpc.method", "/oteldemo.AdService/GetAds");
          logger.info("Non-targeted ad request received, preparing random response.");
          allAds = service.getRandomAds();
          adRequestType = AdRequestType.NOT_TARGETED;
          adResponseType = AdResponseType.RANDOM;
        }
        if (allAds.isEmpty()) {
          // Serve random ads.
          allAds = service.getRandomAds();
          adResponseType = AdResponseType.RANDOM;
        }
        span.setAttribute("app.ads.count", allAds.size());
        span.setAttribute("app.ads.ad_request_type", adRequestType.name());
        span.setAttribute("app.ads.ad_response_type", adResponseType.name());

        // Rich ad serving attributes
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        String campaignId = AD_CAMPAIGNS[tlr.nextInt(AD_CAMPAIGNS.length)];
        String placement = AD_PLACEMENTS[tlr.nextInt(AD_PLACEMENTS.length)];
        String exchange = AD_EXCHANGES[tlr.nextInt(AD_EXCHANGES.length)];

        // Campaign and creative attributes
        span.setAttribute("app.ads.campaign_id", campaignId);
        span.setAttribute("app.ads.campaign_budget_remaining_usd", tlr.nextDouble(100, 50000));
        span.setAttribute("app.ads.campaign_daily_spend_usd", tlr.nextDouble(10, 5000));
        span.setAttribute("app.ads.creative_id", "creative-" + UUID.randomUUID().toString().substring(0, 8));
        span.setAttribute("app.ads.creative_version", CREATIVE_VERSIONS[tlr.nextInt(CREATIVE_VERSIONS.length)]);
        span.setAttribute("app.ads.creative_format", AD_FORMATS[tlr.nextInt(AD_FORMATS.length)]);
        span.setAttribute("app.ads.creative_size_bytes", tlr.nextInt(5000, 500000));

        // Placement and rendering
        span.setAttribute("app.ads.placement", placement);
        span.setAttribute("app.ads.placement_priority", tlr.nextInt(1, 10));
        span.setAttribute("app.ads.rendering_engine", RENDERING_ENGINES[tlr.nextInt(RENDERING_ENGINES.length)]);
        span.setAttribute("app.ads.render_time_ms", tlr.nextInt(5, 200));
        span.setAttribute("app.ads.above_fold", tlr.nextBoolean());
        span.setAttribute("app.ads.viewport_visible_pct", tlr.nextInt(0, 101));

        // Auction and bidding
        span.setAttribute("app.ads.exchange", exchange);
        span.setAttribute("app.ads.bid_strategy", BID_STRATEGIES[tlr.nextInt(BID_STRATEGIES.length)]);
        span.setAttribute("app.ads.bid_amount_usd", Math.round(tlr.nextDouble(0.01, 15.0) * 1000.0) / 1000.0);
        span.setAttribute("app.ads.winning_bid_usd", Math.round(tlr.nextDouble(0.01, 12.0) * 1000.0) / 1000.0);
        span.setAttribute("app.ads.second_price_usd", Math.round(tlr.nextDouble(0.005, 10.0) * 1000.0) / 1000.0);
        span.setAttribute("app.ads.auction_participants", tlr.nextInt(2, 30));
        span.setAttribute("app.ads.auction_duration_ms", tlr.nextInt(5, 100));
        span.setAttribute("app.ads.floor_price_usd", Math.round(tlr.nextDouble(0.001, 2.0) * 1000.0) / 1000.0);
        span.setAttribute("app.ads.impression_cost_usd", Math.round(tlr.nextDouble(0.001, 5.0) * 10000.0) / 10000.0);

        // Targeting and audience
        span.setAttribute("app.ads.targeting_method", TARGETING_METHODS[tlr.nextInt(TARGETING_METHODS.length)]);
        span.setAttribute("app.ads.audience_segment", AUDIENCE_SEGMENTS[tlr.nextInt(AUDIENCE_SEGMENTS.length)]);
        span.setAttribute("app.ads.audience_size_estimate", tlr.nextInt(1000, 5000000));
        span.setAttribute("app.ads.targeting_signals_count", tlr.nextInt(5, 50));
        span.setAttribute("app.ads.advertiser_vertical", ADVERTISER_VERTICALS[tlr.nextInt(ADVERTISER_VERTICALS.length)]);
        span.setAttribute("app.ads.geo_target", new String[]{"US", "US", "GB", "DE", "FR", "JP", "global"}[tlr.nextInt(7)]);
        span.setAttribute("app.ads.device_target", new String[]{"all", "desktop", "mobile", "tablet"}[tlr.nextInt(4)]);

        // Quality and brand safety
        span.setAttribute("app.ads.quality_tier", AD_QUALITY_TIERS[tlr.nextInt(AD_QUALITY_TIERS.length)]);
        span.setAttribute("app.ads.quality_score", tlr.nextInt(1, 11));
        span.setAttribute("app.ads.viewability_prediction", VIEWABILITY_PREDICTIONS[tlr.nextInt(VIEWABILITY_PREDICTIONS.length)]);
        span.setAttribute("app.ads.brand_safety_level", BRAND_SAFETY_LEVELS[tlr.nextInt(BRAND_SAFETY_LEVELS.length)]);
        span.setAttribute("app.ads.content_category_safe", tlr.nextDouble() > 0.02);
        span.setAttribute("app.ads.fraud_detection_score", Math.round(tlr.nextDouble(0, 1) * 1000.0) / 1000.0);
        span.setAttribute("app.ads.invalid_traffic_pct", Math.round(tlr.nextDouble(0, 5) * 100.0) / 100.0);

        // Frequency and pacing
        span.setAttribute("app.ads.frequency_cap_remaining", tlr.nextInt(0, 20));
        span.setAttribute("app.ads.user_impressions_24h", tlr.nextInt(0, 50));
        span.setAttribute("app.ads.user_impressions_7d", tlr.nextInt(0, 200));
        span.setAttribute("app.ads.pacing_status", new String[]{"on-track", "under-pacing", "over-pacing"}[tlr.nextInt(3)]);
        span.setAttribute("app.ads.daily_budget_pct_spent", tlr.nextInt(0, 101));

        // Prediction and ML
        span.setAttribute("app.ads.predicted_ctr", Math.round(tlr.nextDouble(0.001, 0.15) * 10000.0) / 10000.0);
        span.setAttribute("app.ads.predicted_cvr", Math.round(tlr.nextDouble(0.0001, 0.05) * 100000.0) / 100000.0);
        span.setAttribute("app.ads.relevance_score", Math.round(tlr.nextDouble(0, 1) * 1000.0) / 1000.0);
        span.setAttribute("app.ads.ml_model_version", "ad-rank-v" + tlr.nextInt(1, 6) + "." + tlr.nextInt(0, 20));
        span.setAttribute("app.ads.feature_vector_dim", tlr.nextInt(64, 512));
        span.setAttribute("app.ads.inference_latency_ms", tlr.nextInt(1, 50));

        // Infrastructure
        span.setAttribute("app.infra.handler_instance", "ad-service-" + tlr.nextInt(0, 8));
        span.setAttribute("app.infra.heap_used_mb", tlr.nextInt(128, 2048));
        span.setAttribute("app.infra.gc_pause_ms", tlr.nextInt(0, 100));
        span.setAttribute("app.infra.thread_pool_active", tlr.nextInt(1, 50));
        span.setAttribute("app.infra.thread_pool_queue_size", tlr.nextInt(0, 200));
        span.setAttribute("app.infra.connection_pool_active", tlr.nextInt(1, 100));
        span.setAttribute("app.infra.cache_hit_rate", Math.round(tlr.nextDouble(0.5, 0.99) * 1000.0) / 1000.0);

        // Extended ad serving - creative and media
        span.setAttribute("app.ads.creative.width_px", new int[]{728, 300, 160, 320, 250, 970}[tlr.nextInt(6)]);
        span.setAttribute("app.ads.creative.height_px", new int[]{90, 250, 600, 50, 250, 250}[tlr.nextInt(6)]);
        span.setAttribute("app.ads.creative.file_type", new String[]{"image/png", "image/jpeg", "image/webp", "image/gif", "video/mp4", "text/html"}[tlr.nextInt(6)]);
        span.setAttribute("app.ads.creative.load_time_ms", tlr.nextInt(10, 500));
        span.setAttribute("app.ads.creative.cdn_origin", new String[]{"cloudfront-us", "cloudfront-eu", "cloudflare", "fastly", "akamai"}[tlr.nextInt(5)]);
        span.setAttribute("app.ads.creative.cached_at_edge", tlr.nextBoolean());
        span.setAttribute("app.ads.creative.a_b_variant", new String[]{"original", "variant-a", "variant-b", "seasonal"}[tlr.nextInt(4)]);
        span.setAttribute("app.ads.creative.color_scheme", new String[]{"light", "dark", "brand-primary", "seasonal-holiday"}[tlr.nextInt(4)]);
        span.setAttribute("app.ads.creative.cta_text", new String[]{"Shop Now", "Learn More", "Buy Today", "Get 50% Off", "Free Shipping", "Limited Time"}[tlr.nextInt(6)]);
        span.setAttribute("app.ads.creative.has_animation", tlr.nextBoolean());
        span.setAttribute("app.ads.creative.accessibility_alt_text", tlr.nextBoolean());

        // Advertiser and billing context
        span.setAttribute("app.ads.advertiser.id", "adv-" + tlr.nextInt(1000, 9999));
        span.setAttribute("app.ads.advertiser.name", new String[]{"AstroCorp", "SkyView Inc", "OpticsWorld", "StarGazer Ltd", "TeleStar"}[tlr.nextInt(5)]);
        span.setAttribute("app.ads.advertiser.account_type", new String[]{"self-serve", "managed", "agency", "enterprise"}[tlr.nextInt(4)]);
        span.setAttribute("app.ads.advertiser.billing_type", new String[]{"prepaid", "postpaid", "credit-line", "invoice"}[tlr.nextInt(4)]);
        span.setAttribute("app.ads.advertiser.payment_status", new String[]{"current", "current", "current", "overdue", "suspended"}[tlr.nextInt(5)]);
        span.setAttribute("app.ads.advertiser.total_spend_usd", Math.round(tlr.nextDouble(100, 500000) * 100.0) / 100.0);
        span.setAttribute("app.ads.advertiser.lifetime_impressions", tlr.nextLong(1000, 100000000));

        // User intent and contextual signals
        span.setAttribute("app.ads.signal.page_category", new String[]{"product", "category", "search", "homepage", "cart", "blog"}[tlr.nextInt(6)]);
        span.setAttribute("app.ads.signal.user_intent", new String[]{"browsing", "comparing", "ready-to-buy", "researching", "returning"}[tlr.nextInt(5)]);
        span.setAttribute("app.ads.signal.session_depth", tlr.nextInt(1, 20));
        span.setAttribute("app.ads.signal.time_on_site_sec", tlr.nextInt(10, 3600));
        span.setAttribute("app.ads.signal.pages_viewed", tlr.nextInt(1, 30));
        span.setAttribute("app.ads.signal.search_query_present", tlr.nextBoolean());
        span.setAttribute("app.ads.signal.cart_value_usd", Math.round(tlr.nextDouble(0, 2000) * 100.0) / 100.0);
        span.setAttribute("app.ads.signal.returning_customer", tlr.nextBoolean());

        // Attribution and measurement
        span.setAttribute("app.ads.attribution.window_days", new int[]{1, 7, 14, 28, 30}[tlr.nextInt(5)]);
        span.setAttribute("app.ads.attribution.model", new String[]{"last-click", "first-click", "linear", "time-decay", "data-driven"}[tlr.nextInt(5)]);
        span.setAttribute("app.ads.attribution.view_through", tlr.nextBoolean());
        span.setAttribute("app.ads.attribution.click_through", tlr.nextBoolean());
        span.setAttribute("app.ads.measurement.viewable", tlr.nextDouble() > 0.15);
        span.setAttribute("app.ads.measurement.in_view_time_ms", tlr.nextInt(0, 30000));
        span.setAttribute("app.ads.measurement.mrc_viewable", tlr.nextDouble() > 0.2);

        // Regulatory and compliance
        span.setAttribute("app.ads.compliance.gdpr_consent", tlr.nextBoolean());
        span.setAttribute("app.ads.compliance.ccpa_opt_out", tlr.nextDouble() < 0.1);
        span.setAttribute("app.ads.compliance.coppa_applicable", false);
        span.setAttribute("app.ads.compliance.ads_txt_verified", tlr.nextBoolean());
        span.setAttribute("app.ads.compliance.sellers_json_verified", tlr.nextBoolean());

        adRequestsCounter.add(
            1,
            Attributes.of(
                adRequestTypeKey, adRequestType.name(), adResponseTypeKey, adResponseType.name()));

        // Throw 1/10 of the time to simulate a failure when the feature flag is enabled
        if (ffClient.getBooleanValue(AD_FAILURE, false, evaluationContext) && random.nextInt(10) == 0) {
          throw new StatusRuntimeException(Status.UNAVAILABLE);
        }

        if (ffClient.getBooleanValue(AD_MANUAL_GC_FEATURE_FLAG, false, evaluationContext)) {
          logger.warn("Feature Flag " + AD_MANUAL_GC_FEATURE_FLAG + " enabled, performing a manual gc now");
          GarbageCollectionTrigger gct = new GarbageCollectionTrigger();
          gct.doExecute();
        }

        AdResponse reply = AdResponse.newBuilder().addAllAds(allAds).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } catch (StatusRuntimeException e) {
        span.addEvent(
            "Error", Attributes.of(AttributeKey.stringKey("exception.message"), e.getMessage()));
        span.setStatus(StatusCode.ERROR);
        ThreadContext.put("ad.error_type", "StatusRuntimeException");
        ThreadContext.put("ad.error_status", e.getStatus().toString());
        logger.log(Level.WARN, "GetAds Failed with status {}", e.getStatus());
        responseObserver.onError(e);
      } finally {
        ThreadContext.clearAll();
      }
    }
  }

  private static final ImmutableListMultimap<String, Ad> adsMap = createAdsMap();

  @WithSpan("getAdsByCategory")
  private Collection<Ad> getAdsByCategory(@SpanAttribute("app.ads.category") String category) {
    Collection<Ad> ads = adsMap.get(category);
    Span.current().setAttribute("app.ads.count", ads.size());
    return ads;
  }

  private static final Random random = new Random();

  private List<Ad> getRandomAds() {

    List<Ad> ads = new ArrayList<>(MAX_ADS_TO_SERVE);

    // create and start a new span manually
    Span span = tracer.spanBuilder("getRandomAds").startSpan();

    // put the span into context, so if any child span is started the parent will be set properly
    try (Scope ignored = span.makeCurrent()) {

      Collection<Ad> allAds = adsMap.values();
      for (int i = 0; i < MAX_ADS_TO_SERVE; i++) {
        ads.add(Iterables.get(allAds, random.nextInt(allAds.size())));
      }
      span.setAttribute("app.ads.count", ads.size());

    } finally {
      span.end();
    }

    return ads;
  }

  private static AdService getInstance() {
    return service;
  }

  /** Await termination on the main thread since the grpc library uses daemon threads. */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  private static ImmutableListMultimap<String, Ad> createAdsMap() {
    Ad binoculars =
        Ad.newBuilder()
            .setRedirectUrl("/product/2ZYFJ3GM2N")
            .setText("Roof Binoculars for sale. 50% off.")
            .build();
    Ad explorerTelescope =
        Ad.newBuilder()
            .setRedirectUrl("/product/66VCHSJNUP")
            .setText("Starsense Explorer Refractor Telescope for sale. 20% off.")
            .build();
    Ad colorImager =
        Ad.newBuilder()
            .setRedirectUrl("/product/0PUK6V6EV0")
            .setText("Solar System Color Imager for sale. 30% off.")
            .build();
    Ad opticalTube =
        Ad.newBuilder()
            .setRedirectUrl("/product/9SIQT8TOJO")
            .setText("Optical Tube Assembly for sale. 10% off.")
            .build();
    Ad travelTelescope =
        Ad.newBuilder()
            .setRedirectUrl("/product/1YMWWN1N4O")
            .setText(
                "Eclipsmart Travel Refractor Telescope for sale. Buy one, get second kit for free")
            .build();
    Ad solarFilter =
        Ad.newBuilder()
            .setRedirectUrl("/product/6E92ZMYYFZ")
            .setText("Solar Filter for sale. Buy two, get third one for free")
            .build();
    Ad cleaningKit =
        Ad.newBuilder()
            .setRedirectUrl("/product/L9ECAV7KIM")
            .setText("Lens Cleaning Kit for sale. Buy one, get second one for free")
            .build();
    return ImmutableListMultimap.<String, Ad>builder()
        .putAll("binoculars", binoculars)
        .putAll("telescopes", explorerTelescope)
        .putAll("accessories", colorImager, solarFilter, cleaningKit)
        .putAll("assembly", opticalTube)
        .putAll("travel", travelTelescope)
        // Keep the books category free of ads to ensure the random code branch is tested
        .build();
  }

  /** Main launches the server from the command line. */
  public static void main(String[] args) throws IOException, InterruptedException {
    // Start the RPC server. You shouldn't see any output from gRPC before this.
    logger.info("Ad service starting.");
    final AdService service = AdService.getInstance();
    service.start();
    service.blockUntilShutdown();
  }
}
