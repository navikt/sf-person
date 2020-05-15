package no.nav.sf.person

import java.io.File
import mu.KotlinLogging

private val log = KotlinLogging.logger { }

/**
 * data class Vault contains expected vault configuration
 * data class EnvVar contains expected environment variables
 *
 * data class Params contains the previous ones and a set of extension function
 *
 * ext fun getSalesforceDetails is a little bit involving due to support of the following cases
 * 1) local Mock execution
 * 2) local execution towards a scratch org
 * 3) local execution towards preprod org, using local kafka
 * 4) nais preprod execution towards preprod org
 * 5) nais prod execution towards prod org
 *
 * The involving part is due to parameter details and their relationships
 *
 */

const val PROGNAME = "sf-person"

const val SF_MOCK_URL = "http://localhost:6767"
const val SF_MOCK_CID = "validClientID"
const val SF_MOCK_UN = "validUsername"

const val SF_TEST_URL = "https://test.salesforce.com"

const val SF_SCRATCH_CID = "3MVG9Lu3LaaTCEgLHlx_0Tkvl.0NmyxxdEeyOQ0u9qreBe.3gLxK0oY0haUqSyoqynXbv7yVB8_aauHN4fyaU"
const val SF_SCRATCH_UN = "permiteringsvarsel.integrasjon@scratch.org"

const val SF_PREPROD_CID = "3MVG9Ve.2wqUVx_Z3N2_xrMOSIUIieypuK3sIpU_eUu0yFP6gAG.sKxs.wiVC57zbg5067zsRReuFCPyHdKfAx"
const val SF_PREPROD_UN = "permitteringsvarsler.integrasjon@nav.no.preprod"

const val SF_PROD_URL = "https://login.salesforce.com"

const val KS_PWD = "password"
const val KS_PK_ALIAS = "test"
const val KS_PK_PWD = "password"

sealed class IntegrityBase
data class IntegrityIssue(val cause: String) : IntegrityBase()
object IntegrityOk : IntegrityBase()

data class Params(
    val vault: Vault = Vault(),
    val prevVault: Vault = Vault(),
    val envVar: EnvVar = EnvVar(),
    val sfAuthorization: AuthorizationBase = AuthorizationMissing
) {
    fun getSalesforceDetails(): Salesforce = Salesforce(
            instancetype = envVar.sfInstanceType.getSFInstanceType(),
            url = when (envVar.sfInstanceType.getSFInstanceType()) {
                SalesforceInstancetype.MOCK -> SF_MOCK_URL
                SalesforceInstancetype.SCRATCH -> SF_TEST_URL
                SalesforceInstancetype.PREPROD -> SF_TEST_URL
                SalesforceInstancetype.PRODUCTION -> SF_PROD_URL
            },
            version = envVar.sfVersion,
            clientID = when (envVar.sfInstanceType.getSFInstanceType()) {
                SalesforceInstancetype.MOCK -> SF_MOCK_CID
                SalesforceInstancetype.SCRATCH -> SF_SCRATCH_CID
                SalesforceInstancetype.PREPROD -> if (vault.sfClientID.isEmpty()) SF_PREPROD_CID else vault.sfClientID
                SalesforceInstancetype.PRODUCTION -> vault.sfClientID
            },
            username = when (envVar.sfInstanceType.getSFInstanceType()) {
                SalesforceInstancetype.MOCK -> SF_MOCK_UN
                SalesforceInstancetype.SCRATCH -> SF_SCRATCH_UN
                SalesforceInstancetype.PREPROD -> if (vault.sfUsername.isEmpty()) SF_PREPROD_UN else vault.sfUsername
                SalesforceInstancetype.PRODUCTION -> vault.sfUsername
            },
            keystore = getKeystoreDetails(envVar.sfInstanceType.getSFInstanceType())
    )

    private fun String.getSFInstanceType(): SalesforceInstancetype = runCatching {
        SalesforceInstancetype.valueOf(this)
    }
            .getOrDefault(SalesforceInstancetype.MOCK)

    private fun getKSB64(sfit: SalesforceInstancetype) = when (sfit) {
        SalesforceInstancetype.MOCK -> "testkeystorejksB64".getResourceOrDefault()
        SalesforceInstancetype.SCRATCH -> "testkeystorejksB64".getResourceOrDefault()
        SalesforceInstancetype.PREPROD ->
            if (vault.keystoreB64.isEmpty()) "testkeystorejksB64".getResourceOrDefault() else vault.keystoreB64
        SalesforceInstancetype.PRODUCTION -> vault.keystoreB64
    }

    private fun getKSPwd(sfit: SalesforceInstancetype) = when (sfit) {
        SalesforceInstancetype.MOCK -> KS_PWD
        SalesforceInstancetype.SCRATCH -> KS_PWD
        SalesforceInstancetype.PREPROD -> if (vault.ksPassword.isEmpty()) KS_PWD else vault.ksPassword
        SalesforceInstancetype.PRODUCTION -> vault.ksPassword
    }

    private fun getPKAlias(sfit: SalesforceInstancetype) = when (sfit) {
        SalesforceInstancetype.MOCK -> KS_PK_ALIAS
        SalesforceInstancetype.SCRATCH -> KS_PK_ALIAS
        SalesforceInstancetype.PREPROD -> if (vault.pkAlias.isEmpty()) KS_PK_ALIAS else vault.pkAlias
        SalesforceInstancetype.PRODUCTION -> vault.pkAlias
    }

    private fun getPKPwd(sfit: SalesforceInstancetype) = when (sfit) {
        SalesforceInstancetype.MOCK -> KS_PK_PWD
        SalesforceInstancetype.SCRATCH -> KS_PK_PWD
        SalesforceInstancetype.PREPROD -> if (vault.pkPwd.isEmpty()) KS_PK_PWD else vault.pkPwd
        SalesforceInstancetype.PRODUCTION -> vault.pkPwd
    }

    private fun getKeystoreDetails(sfit: SalesforceInstancetype): KeystoreBase =
            KeystoreBase.getPrivatekey(
                    getKSB64(sfit),
                    getKSPwd(sfit),
                    getPKAlias(sfit),
                    getPKPwd(sfit)
            )

    private fun kafkaSecurityConfigOk(): Boolean =
            envVar.kSecProt.isNotEmpty() && envVar.kSaslMec.isNotEmpty() &&
                    vault.kafkaUser.isNotEmpty() && vault.kafkaPassword.isNotEmpty()

    private fun kafkaBaseConfigOk(): Boolean =
            envVar.kBrokers.isNotEmpty() && envVar.kClientID.isNotEmpty() && envVar.kTopic.isNotEmpty()

    fun getKafkaTopics(delim: String = ","): List<String> = envVar.kTopic.let { topic ->
        if (topic.contains(delim))
            topic.split(delim).map { it.trim() } else listOf(topic.trim())
    }

    fun integrityCheck(): IntegrityBase =
            when {
                !getSalesforceDetails().configIsOk() -> IntegrityIssue("Salesforce base config is incomplete")
                !kafkaBaseConfigOk() -> IntegrityIssue("Kafka base config is incomplete")
                envVar.kSecurityEnabled && !kafkaSecurityConfigOk() ->
                    IntegrityIssue("Kafka security enabled, but incomplete kafka security properties")
                !getSalesforceDetails().keystore.signCheckIsOk() -> IntegrityIssue("Keystore and/or config. failure")
                else -> IntegrityOk
            }
}

// according to dev and prod.yaml
const val pathSecrets = "/var/run/secrets/nais.io/vault/"
const val pathServiceUser = "/var/run/secrets/nais.io/serviceuser/"

data class Vault(
        // salesforce details
    val sfClientID: String = getSecretOrDefault("SFClientID"),
    val sfUsername: String = getSecretOrDefault("SFUsername"),

        // kafka details
    val kafkaUser: String = getServiceUserOrDefault("username"),
    val kafkaPassword: String = getServiceUserOrDefault("password"),

        // keystore details
    val keystoreB64: String = getSecretOrDefault("keystoreJKSB64"),
    val ksPassword: String = getSecretOrDefault("KeystorePassword"),
    val pkAlias: String = getSecretOrDefault("PrivateKeyAlias"),
    val pkPwd: String = getSecretOrDefault("PrivateKeyPassword"),

    val personFilter: FilterPersonBase = FilterPerson.fromJson(getSecretOrDefault("personFilter"))
) {
    companion object {
        private fun getOrDefault(file: File, d: String): String = runCatching { file.readText(Charsets.UTF_8) }
                .onFailure { log.error { "Couldn't read ${file.absolutePath}" } }
                .getOrDefault(d)

        fun getSecretOrDefault(k: String, d: String = "", p: String = pathSecrets): String =
                getOrDefault(File("$p$k"), d)

        fun getServiceUserOrDefault(k: String, d: String = "", p: String = pathServiceUser): String =
                getOrDefault(File("$p$k"), d)
    }
}

data class EnvVar(
        // salesforce details
    val sfInstanceType: String = getEnvOrDefault("SF_INSTTYPE", "MOCK").toUpperCase(),
    val sfVersion: String = getEnvOrDefault("SF_VERSION", "v48.0"),

        // kafka details
    val kBrokers: String = getEnvOrDefault("KAFKA_BROKERS", "localhost:9092"),
    val kClientID: String = getEnvOrDefault("KAFKA_CLIENTID", PROGNAME),
    val kSecurityEnabled: Boolean = getEnvOrDefault("KAFKA_SECURITY", "false").toBoolean(),
    val kSecProt: String = getEnvOrDefault("KAFKA_SECPROT"),
    val kSaslMec: String = getEnvOrDefault("KAFKA_SASLMEC"),
    val kTopic: String = getEnvOrDefault("KAFKA_TOPIC", PROGNAME),

        // other
    val httpsProxy: String = getEnvOrDefault("HTTPS_PROXY"),
    val msBetweenWork: Long = getEnvOrDefault("MS_BETWEEN_WORK", "60000").toLong()
) {
    companion object {
        fun getEnvOrDefault(k: String, d: String = ""): String = runCatching { System.getenv(k) ?: d }.getOrDefault(d)
    }
}

internal fun String.getResourceOrDefault(d: String = ""): String =
        runCatching { Params::class.java.getResourceAsStream("/$this").bufferedReader().use { it.readText() } }
                .getOrDefault(d)
