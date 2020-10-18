package no.nav.sf.person

import java.io.File
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.PROGNAME

private val log = KotlinLogging.logger {}

const val EV_kafkaConsumerTopic = "KAFKA_TOPIC"
val kafkaPersonEventTopic = AnEnvironment.getEnvOrDefault(EV_kafkaConsumerTopic, "$PROGNAME-consumer")

const val TARGET = "1000060614281"

internal fun investigate(ws: WorkSettings) {
    var result: MutableList<String> = mutableListOf()
    log.info { "Investigate - start" }
    val kafkaConsumer = AKafkaConsumer<ByteArray, ByteArray>(
            config = ws.kafkaConfigAlternative, // Separate clientId - do not affect offset of normal read
            fromBeginning = true,
            topics = listOf(kafkaPersonEventTopic)
    )

    var msg: String = ""
    workMetrics.noOfInvestigatedEvents.clear()

    kafkaConsumer.consume { consumerRecords ->

        if (consumerRecords.isEmpty) {
            return@consume KafkaConsumerStates.IsFinished.also { log.info { "Investigate finished - no more messages" } }
        }

        workMetrics.noOfInvestigatedEvents.inc(consumerRecords.count().toDouble())

        /*
        val pTypes = consumerRecords.map {
            PersonBase.fromProto(it.key(), it.value()).also { pb ->
                if (pb is PersonProtobufIssue)
                    log.error { "Investigate - Protobuf parsing issue for offset ${it.offset()} in partition ${it.partition()}" }
            }
        }*/

        consumerRecords.filter { PersonProto.PersonKey.parseFrom(it.key()).aktoerId == TARGET }.forEach {
            log.info { "Investigate - found target in key" }

            val pType = PersonBase.fromProto(it.key(), it.value()).also { pb ->
                    if (pb is PersonProtobufIssue)
                        log.error { "Investigate - Protobuf parsing issue for offset ${it.offset()} in partition ${it.partition()}" }
            }

            if (pType is PersonProtobufIssue) {
                log.error { "Investigate - Protobuf issues - leaving kafka consumer loop" }
                workMetrics.consumerIssues.inc()
                return@consume KafkaConsumerStates.HasIssues
            }

            if (pType is PersonTombstone) {
                log.info { "Investigate - found tombstone" }
                result.add("${it.offset()}: Tombstone")
            }

            if (pType is Person) {
                log.info { "Investigate - found person" }
                result.add("${it.offset()}:" + pType.toJson())
            }
        }

        /*
        if (pTypes.filterIsInstance<PersonProtobufIssue>().isNotEmpty()) {
            log.error { "Investigate - Protobuf issues - leaving kafka consumer loop" }
            workMetrics.consumerIssues.inc()
            return@consume KafkaConsumerStates.HasIssues
        }

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

         */
        KafkaConsumerStates.IsOk
    }

    result.forEach { msg += "\n$it" }
    log.info { "Investigate - Attempt file storage" }
    File("/tmp/investigate").writeText("Result: $msg")
    log.info { "Investigate - File storage done" }
}
