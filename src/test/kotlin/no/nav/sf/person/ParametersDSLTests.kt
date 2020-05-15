package no.nav.sf.person

import io.kotest.assertions.asClue
import io.kotest.core.spec.style.StringSpec
import io.kotest.extensions.system.withEnvironment
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import java.io.File

class ParametersDSLTests : StringSpec() {

    // use ./src/test/resources for both secrets and service user
    private val vaultPath = "/src/test/resources/"

    private val default = ""
    private val anotherDefault = "something"

    init {

        "data class Vault should work as expected" {

            val sfClientID = "SFClientID"
            val sfClientIDValue = "Hi there!"

            val username = "username"
            val usernameValue = "Roger Waters"

            Vault.getSecretOrDefault(sfClientID) shouldBe default
            Vault.getServiceUserOrDefault(username) shouldBe default

            Vault.getSecretOrDefault(sfClientID, anotherDefault) shouldBe anotherDefault
            Vault.getServiceUserOrDefault(username, anotherDefault) shouldBe anotherDefault

            Vault.getSecretOrDefault(
                    sfClientID,
                    anotherDefault,
                    "${System.getProperty("user.dir")}$vaultPath") shouldBe sfClientIDValue

            Vault.getServiceUserOrDefault(
                    username,
                    anotherDefault,
                    "${System.getProperty("user.dir")}$vaultPath") shouldBe usernameValue
        }

        "data class EnvVar should work as expected" {

            val nEnvVar = "non-existing-env-var"

            val eEnvVar = "existing-env-var"
            val eEnvVarValue = "YES!"

            withEnvironment(mapOf(eEnvVar to eEnvVarValue)) {
                EnvVar.getEnvOrDefault(nEnvVar) shouldBe default
                EnvVar.getEnvOrDefault(nEnvVar, anotherDefault) shouldBe anotherDefault
                EnvVar.getEnvOrDefault(eEnvVar, anotherDefault) shouldBe eEnvVarValue
            }
        }

        "data class Params should work as expected" {

            // default Params should refer to MOCK situation
            Params()
                    .also {
                        it.integrityCheck().shouldBeInstanceOf<IntegrityOk>()
                    }
                    .getSalesforceDetails()
                    .asClue {
                        it.instancetype shouldBe SalesforceInstancetype.MOCK
                        it.clientID shouldBe SF_MOCK_CID
                        it.username shouldBe SF_MOCK_UN
                        it.keystore.signCheckIsOk() shouldBe true
                    }

            Params(vault = Vault(), envVar = EnvVar(sfInstanceType = "SCRATCH"))
                    .also {
                        it.integrityCheck().shouldBeInstanceOf<IntegrityOk>()
                    }
                    .getSalesforceDetails().asClue {
                        it.instancetype shouldBe SalesforceInstancetype.SCRATCH
                        it.clientID shouldBe SF_SCRATCH_CID
                        it.username shouldBe SF_SCRATCH_UN
                        it.keystore.signCheckIsOk() shouldBe true
                    }

            Params(vault = Vault(), envVar = EnvVar(sfInstanceType = "PREPROD"))
                    .also {
                        it.integrityCheck().shouldBeInstanceOf<IntegrityOk>()
                    }
                    .getSalesforceDetails().asClue {
                        it.instancetype shouldBe SalesforceInstancetype.PREPROD
                        it.clientID shouldBe SF_PREPROD_CID
                        it.username shouldBe SF_PREPROD_UN
                        it.keystore.signCheckIsOk() shouldBe true
                    }

            Params(vault = Vault(), envVar = EnvVar(sfInstanceType = "PRODUCTION"))
                    .also {
                        it.integrityCheck().shouldBeInstanceOf<IntegrityIssue>()
                    }
                    .getSalesforceDetails().asClue {
                        it.instancetype shouldBe SalesforceInstancetype.PRODUCTION
                        it.clientID shouldBe default
                        it.username shouldBe default
                        it.keystore.signCheckIsOk() shouldBe false
                    }

            // defaulting to MOCK in case of invalid enum value
            Params(vault = Vault(), envVar = EnvVar(sfInstanceType = "INVALID")).getSalesforceDetails().asClue {
                it.instancetype shouldBe SalesforceInstancetype.MOCK
                it.clientID shouldBe SF_MOCK_CID
                it.username shouldBe SF_MOCK_UN
                it.keystore.signCheckIsOk() shouldBe true
            }
        }

        "fun getResourceOrDefault should work as expected" {

            val path = "${System.getProperty("user.dir")}/src/main/resources/testkeystorejksB64"

            "invalidResource".getResourceOrDefault() shouldBe default
            "invalidResource".getResourceOrDefault(anotherDefault) shouldBe anotherDefault
            "testkeystorejksB64"
                    .getResourceOrDefault(anotherDefault)
                    .hashCode() shouldBeExactly File(path).readText().hashCode()
        }

        "fun getKafkaTopics should work as expected" {

            Params().getKafkaTopics() shouldBe listOf(PROGNAME)

            Params(
                    vault = Vault(),
                    envVar = EnvVar(kTopic = "topic1 ")
            ).getKafkaTopics() shouldBe listOf("topic1")

            Params(
                    vault = Vault(),
                    envVar = EnvVar(kTopic = "topic1, topic2 ")
            ).getKafkaTopics() shouldBe listOf("topic1", "topic2")

            Params(
                    vault = Vault(),
                    envVar = EnvVar(kTopic = "topic1# topic2 ")
            ).getKafkaTopics("#") shouldBe listOf("topic1", "topic2")
        }
    }
}
