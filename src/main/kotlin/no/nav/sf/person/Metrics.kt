package no.nav.sf.person

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.hotspot.DefaultExports
import mu.KotlinLogging

object Metrics {

    private val log = KotlinLogging.logger { }

    val cRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

    val responseLatency: Histogram = Histogram
        .build()
        .name("response_latency_seconds_histogram")
        .help("Salesforce response latency since last restart")
        .register()

    val successfulRequest: Gauge = Gauge
        .build()
        .name("successful_request_gauge")
        .help("No. of successful requests to Salesforce since last restart")
        .register()

    val failedRequest: Gauge = Gauge
        .build()
        .name("failed_request_gauge")
        .help("No. of failed requests to Salesforce since last restart")
        .register()

    val sentPersons: Gauge = Gauge
        .build()
        .name("sent_layoff_gauge")
        .help("No. of layoffs sent to Salesforce in last work session")
        .register()

    val preStopHook: Gauge = Gauge
            .build()
            .name("pre_stop__hook_gauge")
            .help("No. of preStopHook activation since ever")
            .register()

    init {
        DefaultExports.initialize()
        log.info { "Prometheus metrics are ready" }
    }

    fun sessionReset() {
        sentPersons.clear()
    }

    fun resetAll() {
        responseLatency.clear()
        successfulRequest.clear()
        failedRequest.clear()
    }
}
