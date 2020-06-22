package no.nav.sf.person

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.types.shouldBeInstanceOf
import no.nav.pdlsf.proto.PersonProto

class PersonTests : StringSpec() {

    private val protoPK = PersonProto.PersonKey.newBuilder().apply { aktoerId = "1234" }.build().toByteArray()
    private val protoPV = PersonProto.PersonValue.newBuilder().apply {
        identifikasjonsnummer = "678"
        fornavn = "Fornavn"
        mellomnavn = "Mellomnavn"
        etternavn = "Etternavn"
        adressebeskyttelse = PersonProto.PersonValue.Gradering.STRENGT_FORTROLIG_UTLAND
        kommunenummer = "1104"
        region = "11"
        doed = false
    }.build().toByteArray()

    init {

        "fun fromProto should work as expected" {

            PersonBase.fromProto(protoPK, protoPV).shouldBeInstanceOf<Person>()

            PersonBase.fromProto(
                    "invalid".toByteArray(),
                    "invalid".toByteArray()
            ).shouldBeInstanceOf<PersonProtobufIssue>()

            PersonBase.fromProto(protoPK, null).shouldBeInstanceOf<PersonTombstone>()

            PersonBase.fromProto("invalid".toByteArray(), null).shouldBeInstanceOf<PersonProtobufIssue>()
        }
    }
}
