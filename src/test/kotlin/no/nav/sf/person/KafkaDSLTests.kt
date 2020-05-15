package no.nav.sf.person

import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.matchers.shouldBe
import no.nav.common.JAASCredential
import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

class KafkaDSLTests : StringSpec() {

    private val topicString = "topicString"
    private val topicByteArray = "topicByteArray"
    private val kuP1 = JAASCredential("srvkafkap1", "kafkap1")
    private val kuC1 = JAASCredential("srvkafkac1", "kafkac1")
    private val ke = KafkaEnvironment(
        topicNames = listOf(topicString, topicByteArray),
        withSecurity = true,
        users = listOf(kuP1, kuC1),
        autoStart = true
    )
        .addProducerToTopic(kuP1.username, topicString)
        .addProducerToTopic(kuP1.username, topicByteArray)
        .addConsumerToTopic(kuC1.username, topicString)
        .addConsumerToTopic(kuC1.username, topicByteArray)

    private val dataSet = setOf("event1", "event2", "event3")

    init {

        "KafkaDSL should not invoke doConsume when invalid config" {

            getKafkaConsumerByConfig<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
                    // missing some config
                ),
                listOf(topicString)
            ) {
                // no invocation
                true shouldBe false
                ConsumerStates.IsFinished
            } shouldBe false
        }

        "KafkaDSL should invoke doConsume for non authe/autho user" {

            getKafkaConsumerByConfig<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.GROUP_ID_CONFIG to "TEST",
                    ConsumerConfig.CLIENT_ID_CONFIG to "TEST",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                ).addKafkaSecurity("invalidUser", "invalidPwd"),
                listOf(topicString)
            ) { cRecords ->
                cRecords.fold(true) { acc, r -> acc && dataSet.contains(r.value()) } shouldBe false
                if (!cRecords.isEmpty) ConsumerStates.IsOkNoCommit else ConsumerStates.IsFinished
            } shouldBe true
        }

        "KafkaDSL should be able to consume events from topic" {

            getKafkaProducerByConfig<String, String>(
                mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                    ProducerConfig.ACKS_CONFIG to "all",
                    ProducerConfig.CLIENT_ID_CONFIG to "TEST"
                ).addKafkaSecurity(kuP1.username, kuP1.password)
            ) {
                dataSet
                    .fold(true) { acc, s -> acc && send(topicString, "", s) } shouldBe true
            } shouldBe true

            getKafkaConsumerByConfig<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.GROUP_ID_CONFIG to "TEST",
                    ConsumerConfig.CLIENT_ID_CONFIG to "TEST",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "true"
                ).addKafkaSecurity(kuC1.username, kuC1.password),
                listOf(topicString)
            ) { cRecords ->
                cRecords.fold(true) { acc, r -> acc && dataSet.contains(r.value()) } shouldBe true
                if (!cRecords.isEmpty) ConsumerStates.IsOkNoCommit else ConsumerStates.IsFinished
            } shouldBe true
        }

        "KafkaDSL should be able to re-consume events from topic" {

            var noOfRecs = 0

            getKafkaConsumerByConfig<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.GROUP_ID_CONFIG to "TEST",
                    ConsumerConfig.CLIENT_ID_CONFIG to "TEST",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "true"
                ).addKafkaSecurity(kuC1.username, kuC1.password),
                listOf(topicString),
                fromBeginning = true
            ) { cRecords ->
                noOfRecs += cRecords.count()
                cRecords.fold(true) { acc, r -> acc && dataSet.contains(r.value()) } shouldBe true
                if (!cRecords.isEmpty) ConsumerStates.IsOkNoCommit else ConsumerStates.IsFinished
            } shouldBe true

            noOfRecs shouldBe dataSet.size
        }
    }

    override fun beforeTest(testCase: TestCase) {
        super.beforeTest(testCase)
        ShutdownHook.reset()
        ServerState.reset()
    }

    override fun afterSpec(spec: Spec) {
        super.afterSpec(spec)
        ke.tearDown()
    }
}
