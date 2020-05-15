package no.nav.sf.person

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration

internal val json = Json(JsonConfiguration.Stable)

@Serializable
internal data class SFsObjectRest(
    val allOrNone: Boolean = true,
    val records: List<KafkaMessage>
) {
    fun toJson() = json.stringify(serializer(), this)
}

@Serializable
internal data class KafkaMessage(
    val attributes: SFsObjectRestAttributes = SFsObjectRestAttributes(),
    @SerialName("CRM_Topic__c") val topic: String,
    @SerialName("CRM_Key__c") val key: String,
    @SerialName("CRM_Value__c") val value: String
)

@Serializable
internal data class SFsObjectRestAttributes(
    val type: String = "KafkaMessage__c"
)
