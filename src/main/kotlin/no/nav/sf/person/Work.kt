package no.nav.sf.person

import io.prometheus.client.Gauge
import mu.KotlinLogging
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.KafkaMessage
import no.nav.sf.library.SFsObjectRest
import no.nav.sf.library.SalesforceClient
import no.nav.sf.library.encodeB64
import no.nav.sf.library.isSuccess
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

private val log = KotlinLogging.logger {}

sealed class ExitReason {
    object NoSFClient : ExitReason()
    object NoKafkaClient : ExitReason()
    object NoEvents : ExitReason()
    object Work : ExitReason()
}

data class WorkSettings(
    val kafkaConfig: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java
    ),
    val sfClient: SalesforceClient = SalesforceClient()
)

// some work metrics
data class WMetrics(
    val noOfConsumedEvents: Gauge = Gauge
            .build()
            .name("kafka_consumed_event_gauge")
            .help("No. of consumed activity events from kafka since last work session")
            .register(),
    val noOfPostedEvents: Gauge = Gauge
            .build()
            .name("sf_posted_event_gauge")
            .help("No. of posted events to Salesforce since last work session")
            .register()
) {
    fun clearAll() {
        noOfConsumedEvents.clear()
        noOfPostedEvents.clear()
    }
}

val workMetrics = WMetrics()

internal fun work(ws: WorkSettings): Pair<WorkSettings, ExitReason> {

    log.info { "bootstrap work session starting" }
    workMetrics.clearAll()

    var exitReason: ExitReason = ExitReason.NoSFClient

    ws.sfClient.enablesObjectPost { postActivities ->

        exitReason = ExitReason.NoKafkaClient
        val kafkaConsumer = AKafkaConsumer<ByteArray, ByteArray>(
                config = ws.kafkaConfig,
                fromBeginning = false
        )

        kafkaConsumer.consume { consumerRecords ->

            exitReason = ExitReason.NoEvents
            if (consumerRecords.isEmpty) return@consume KafkaConsumerStates.IsFinished

            exitReason = ExitReason.Work
            workMetrics.noOfConsumedEvents.inc(consumerRecords.count().toDouble())

            val pTypes = consumerRecords.map {
                PersonBase.fromProto(it.key(), it.value()).also { pb ->
                    if (pb is PersonProtobufIssue)
                        log.error { "Protobuf parsing issue for offset ${it.offset()} in partition ${it.partition()}" }
                }
            }

            if (pTypes.filterIsInstance<PersonProtobufIssue>().isNotEmpty()) {
                log.error { "Protobuf issues - leaving kafka consumer loop" }
                return@consume KafkaConsumerStates.HasIssues
            }

            val topic = kafkaConsumer.topics.first()
            val tombstones = pTypes.filterIsInstance<PersonTombstone>()

            val persons = pTypes.filterIsInstance<Person>()

            val body = SFsObjectRest(
                    records = tombstones.map {
                        KafkaMessage(
                                topic = topic,
                                key = it.toPersonKey().toJson().encodeB64(),
                                value = ""
                        )
                    } + persons.map {
                        KafkaMessage(
                                topic = topic,
                                key = it.toPersonKey().toJson().encodeB64(),
                                value = it.toJson().encodeB64()
                        )
                    }
            ).toJson()

            when (postActivities(body).isSuccess()) {
                true -> {
                    workMetrics.noOfPostedEvents.inc(consumerRecords.count().toDouble())
                    KafkaConsumerStates.IsOk
                }
                false -> KafkaConsumerStates.HasIssues
            }
        }
    }
    log.info { "bootstrap work session finished - $exitReason" }

    return Pair(ws, exitReason)
}
