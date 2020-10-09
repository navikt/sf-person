package no.nav.sf.person

import java.io.File
import mu.KotlinLogging
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.PROGNAME

private val log = KotlinLogging.logger {}

const val EV_kafkaConsumerTopic = "KAFKA_TOPIC"
val kafkaPersonEventTopic = AnEnvironment.getEnvOrDefault(EV_kafkaConsumerTopic, "$PROGNAME-consumer")

const val TARGET = "1000060614281"

internal fun investigate(ws: WorkSettings) {
    log.info { "Investigate - start" }
    val kafkaConsumer = AKafkaConsumer<ByteArray, ByteArray>(
            config = ws.kafkaConfigAlternative, // Separate clientId - do not affect offset of normal read
            fromBeginning = true,
            topics = listOf(kafkaPersonEventTopic)
    )

    var msg: String = ""
    workMetrics.noOfInvestigatedEvents.clear()

    kafkaConsumer.consume { consumerRecords ->

        if (consumerRecords.isEmpty) { return@consume KafkaConsumerStates.IsFinished.also { log.info { "Investigate finished - no more messages" } } }

        log.info { "Investigate batch start - nO records ${consumerRecords.count()}" }
        workMetrics.noOfInvestigatedEvents.inc(consumerRecords.count().toDouble())

        log.info { "Investigate batch start start map" }
        val pTypes = consumerRecords.map {
            log.info { "record: k: ${it.key()?.size} v: ${it.value()?.size} " }
            PersonBase.fromProto(it.key(), it.value()).also { pb ->
                if (pb is PersonProtobufIssue)
                    log.error { "Investigate - Protobuf parsing issue for offset ${it.offset()} in partition ${it.partition()}" }
            }
        }

        log.info { "Investigate Batch after pTypes - start" }
        // consumerRecords.filter { PersonProto.PersonKey.parseFrom(it.key()).aktoerId == TARGET }.forEach {
        //    log.info { "Investigate - found target in key" }
        // }

        if (pTypes.filterIsInstance<PersonProtobufIssue>().isNotEmpty()) {
            log.error { "Investigate - Protobuf issues - leaving kafka consumer loop" }
            workMetrics.consumerIssues.inc()
            return@consume KafkaConsumerStates.HasIssues
        }

        val topic = kafkaConsumer.topics.first()
        val tombstones = pTypes.filterIsInstance<PersonTombstone>()

        tombstones.filter { it.aktoerId == TARGET }.forEach {
            log.info { "Investigate - found target as tombstone" }
            msg += "\nFound tombstone"
        }

        val persons = pTypes.filterIsInstance<Person>()

        persons.filter { it.aktoerId == TARGET }.forEach {
            log.info { "Investigate - found target as person" }
            msg += "\nFound as person:\n${it.toJson()}"
        }

        log.info { "Investigate Batch - end" }
        KafkaConsumerStates.IsOk
    }

    log.info { "Investigate - Attempt file storage" }
    File("/tmp/investigate").writeText("Result: $msg")
    log.info { "Investigate - File storage done" }
}
