package no.nav.sf.person

import kotlinx.serialization.Serializable
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto

private val log = KotlinLogging.logger { }

@Serializable
internal data class PersonKey(val aktoer_id: String, val tombstone: Boolean) {
    fun toJson(): String = no.nav.sf.library.json.stringify(serializer(), this)
}

internal sealed class PersonBase {
    companion object {

        private fun createPersonTombstone(key: ByteArray): PersonBase =
                runCatching { PersonTombstone(PersonProto.PersonKey.parseFrom(key).aktoerId) }
                        .getOrDefault(PersonProtobufIssue)

        fun fromProto(key: ByteArray, value: ByteArray?): PersonBase =
                if (value == null) createPersonTombstone(key) else
                    runCatching {
                        PersonProto.PersonValue.parseFrom(value).let { v ->
                            Person(
                                    aktoerId = PersonProto.PersonKey.parseFrom(key).aktoerId,
                                    identifikasjonsnummer = v.identifikasjonsnummer,
                                    fornavn = v.fornavn,
                                    mellomnavn = v.mellomnavn,
                                    etternavn = v.etternavn,
                                    adressebeskyttelse = v.adressebeskyttelse,
                                    sikkerhetstiltak = v.sikkerhetstiltakList,
                                    kommunenummer = v.kommunenummer,
                                    region = v.region,
                                    doed = v.doed
                            )
                        }
                    }.getOrDefault(PersonProtobufIssue)
    }
}
internal object PersonProtobufIssue : PersonBase()
internal data class PersonTombstone(val aktoerId: String) : PersonBase() {
    fun toPersonKey(): PersonKey = PersonKey(this.aktoerId, true)
}

@Serializable
internal data class Person(
    val aktoerId: String,
    val identifikasjonsnummer: String,
    val fornavn: String,
    val mellomnavn: String,
    val etternavn: String,
    val adressebeskyttelse: PersonProto.PersonValue.Gradering,
    val sikkerhetstiltak: List<String>,
    val kommunenummer: String,
    val region: String,
    val doed: Boolean
) : PersonBase() {
    fun toJson(): String = no.nav.sf.library.json.stringify(serializer(), this)
    fun toPersonKey(): PersonKey = PersonKey(this.aktoerId, false)
}
