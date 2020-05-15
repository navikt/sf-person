package no.nav.sf.person

import io.kotest.core.spec.style.StringSpec
import io.kotest.core.test.TestCase
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.ints.shouldBeGreaterThanOrEqual
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.http4k.client.ApacheClient
import org.http4k.core.Method
import org.http4k.core.Request

private val log = KotlinLogging.logger { }

class BootstrapTests : StringSpec() {

    private suspend fun triggerPreStopHook(afterMS: Long) {
        delay(afterMS)
        log.info { "PreStopHook firing!" }
        ApacheClient().invoke(Request(Method.GET, "$NAIS_URL$NAIS_PRESTOP").body(""))
    }

    private suspend fun kafkaProducer(delayBetweenEvents: Long, noOfEvents: Int) {

        getKafkaProducerByConfig<String, String>(
                mapOf(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to EnvVar().kBrokers,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                        ProducerConfig.ACKS_CONFIG to "all",
                        ProducerConfig.CLIENT_ID_CONFIG to PROGNAME
                )
        ) {
            (1..noOfEvents).forEach {
                runBlocking { delay(delayBetweenEvents) }
                send(PROGNAME, "null", "$it")
            }
        }
    }

    init {

        "object Bootstrap should exit when integrity issues" {

            Bootstrap.start(EnvVar(kTopic = ""))
            Metrics.failedRequest.get().toInt() shouldBeExactly 1
        }

        "object Bootstrap should exit when PreStopHook activated".config(enabled = false) {

            launch { triggerPreStopHook(1_000L) }
            Bootstrap.start(EnvVar(msBetweenWork = 10_000))

            Metrics.preStopHook.get().toInt() shouldBeExactly 1
        }

        "object Bootstrap should wait for Salesforce availability".config(enabled = false) {

            launch { triggerPreStopHook(2_000L) }
            Bootstrap.start(EnvVar(msBetweenWork = 250))

            Metrics.failedRequest.get().toInt() shouldBeGreaterThanOrEqual 3
        }

        "object Bootstrap should wait for kafka availability".config(enabled = false) {

            launch { triggerPreStopHook(2_000L) }
            sfMockAPI {
                Bootstrap.start(EnvVar(msBetweenWork = 1_000))
            }

            Metrics.preStopHook.get().toInt() shouldBeExactly 1
        }

        "object Bootstrap should work".config(enabled = false) {

            // prerequisite 1 - availability of scratch org with KafkaMessage__c + connected app
            // prerequisite 2 - availability of local kafka

            val noOfEvents = 50

            launch {
                kafkaProducer(30, noOfEvents)
                triggerPreStopHook(2_000)
            }

            Bootstrap.start(EnvVar(msBetweenWork = 250, sfInstanceType = "SCRATCH"))

            Metrics.failedRequest.get().toInt() shouldBeExactly 0
            Metrics.successfulRequest.get().toInt() shouldBeGreaterThanOrEqual 2
            Metrics.preStopHook.get().toInt() shouldBeExactly 1
        }
    }

    override fun beforeTest(testCase: TestCase) {
        super.beforeTest(testCase)
        Metrics.resetAll()
        Metrics.preStopHook.clear() // clearing instead of counting trigger invocations....
    }
}
