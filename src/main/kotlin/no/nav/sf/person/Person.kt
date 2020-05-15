package no.nav.sf.person

import kotlinx.serialization.Serializable
import mu.KotlinLogging
import no.nav.pdlsf.proto.PersonProto

private val log = KotlinLogging.logger { }

@Serializable
internal data class PersonKey(val aktoer_id: String, val tombstone: Boolean) {
    fun toJson(): String = json.stringify(serializer(), this)
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
                                    aktoer_id = PersonProto.PersonKey.parseFrom(key).aktoerId,
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
internal data class PersonTombstone(val aktoer_id: String) : PersonBase() {
    fun toPersonKey(): PersonKey = PersonKey(this.aktoer_id, true)
}

@Serializable
internal data class Person(
    val aktoer_id: String,
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
    fun toJson(): String = json.stringify(serializer(), this)
    fun toPersonKey(): PersonKey = PersonKey(this.aktoer_id, false)
}

internal fun List<Person>.applyFilter(f: FilterPersonBase): List<Person> = when (f) {
    is FilterPersonInvalid -> emptyList()
    is FilterPerson -> {
        this.filter { p ->
            val rfIndex: Int = f.hasRegion(p.region)
            when (val rf = if (rfIndex == -1) RegionMissing else f.regions[rfIndex]) {
                is RegionMissing -> false
                is Region -> {
                    when {
                        rf.municipals.isNotEmpty() -> rf.municipals.contains(p.kommunenummer) && !p.doed
                        else -> !p.doed
                    }
                }
            }
        }
    }
}

sealed class FilterPersonBase
object FilterPersonInvalid : FilterPersonBase()

@Serializable
internal data class FilterPerson(
    val regions: List<Region>
) : FilterPersonBase() {

    companion object {
        fun fromJson(data: String): FilterPersonBase = runCatching { json.parse(serializer(), data) }
                .onFailure {
                    ServerState.flag(ServerStates.FilterPersonIssues)
                    log.error { "Parsing of person filter in vault failed - ${it.localizedMessage}" }
                }
                .getOrDefault(FilterPersonInvalid)
    }

    fun hasRegion(r: String): Int = regions.map { it.region }.indexOf(r)
}

/**
 * See protobuf schema as reference
 * Only LIVING persons in listed regions and related municipals will be transferred to Salesforce
 * iff empty list of municipal  - all living persons in that region
 * iff non-empty municipals - only living person in given region AND municipals will be transferred
 *
 */

sealed class RegionBase
object RegionMissing : RegionBase()

@Serializable
internal data class Region(
    val region: String,
    val municipals: List<String> = emptyList()
) : RegionBase()
