package no.nav.sf.person

import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

private val log = KotlinLogging.logger {}

internal fun work(p: Params): AuthorizationBase {

    log.info { "bootstrap work session starting" }

    return getSalesforceSObjectPostFun(p.getSalesforceDetails(), p.sfAuthorization) { sfPost ->

        val personFilterChanged = p.vault.personFilter != p.prevVault.personFilter

        getKafkaConsumerByConfig<ByteArray, ByteArray>(
                mapOf(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to p.envVar.kBrokers,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                        ConsumerConfig.GROUP_ID_CONFIG to p.envVar.kClientID,
                        ConsumerConfig.CLIENT_ID_CONFIG to p.envVar.kClientID,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 200, // Use of SF REST API
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                ).let { cMap ->
                    if (p.envVar.kSecurityEnabled)
                        cMap.addKafkaSecurity(
                                p.vault.kafkaUser,
                                p.vault.kafkaPassword,
                                p.envVar.kSecProt,
                                p.envVar.kSaslMec
                        )
                    else cMap
                },
                p.getKafkaTopics().also {
                    log.info { "Subscribing to topic ${p.getKafkaTopics()} with user ${p.vault.kafkaUser}" }
                }, fromBeginning = personFilterChanged
        ) { cRecords ->
            if (!cRecords.isEmpty) {

                val pTypes = cRecords.map {
                    PersonBase.fromProto(it.key(), it.value()).also { pb ->
                        if (pb is PersonProtobufIssue)
                            log.error { "Protobuf parsing issue for offset ${it.offset()} in partition ${it.partition()}" }
                    }
                }

                if (pTypes.filterIsInstance<PersonProtobufIssue>().isNotEmpty()) {
                    log.error { "Protobuf issues - leaving kafka consumer loop" }
                    return@getKafkaConsumerByConfig ConsumerStates.HasIssues
                }

                val topic = p.getKafkaTopics().first()
                val tombstones = pTypes.filterIsInstance<PersonTombstone>()
                val persons = pTypes.filterIsInstance<Person>().applyFilter(p.vault.personFilter)

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

                val noOfPersons = tombstones.size + persons.size

                if (sfPost(body)) {
                    Metrics.sentPersons.inc(noOfPersons.toDouble())
                    log.info { "Post of $noOfPersons person(s) to Salesforce" }

                    // reset alarm metric
                    if (ServerState.isOk()) Metrics.failedRequest.clear()

                    ConsumerStates.IsOk
                } else {
                    log.error { "Couldn't post $noOfPersons persons(s) to Salesforce - leaving kafka consumer loop" }
                    ConsumerStates.HasIssues
                }
            } else {
                log.info { "Kafka events completed for now - leaving kafka consumer loop" }
                ConsumerStates.IsFinished
            }
        }
    }
}
