package no.nav.sf.person

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import no.nav.pdlsf.proto.PersonProto

class PersonTests : StringSpec() {

    // use ./src/test/resources for both secrets and service user
    private val vaultPath = "/src/test/resources/"

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

    private val persons = listOf(
            Person(
                    "1", "1", "f", "m", "e",
                    PersonProto.PersonValue.Gradering.UGRADERT,
                    listOf("t1", "t2"), "0301", "03", false),
            Person(
                    "1", "1", "f", "m", "e",
                    PersonProto.PersonValue.Gradering.UGRADERT,
                    listOf("t1", "t2"), "0302", "03", true),
            Person(
                    "2", "2", "f", "m", "e",
                    PersonProto.PersonValue.Gradering.UGRADERT,
                    listOf("t1", "t2"), "1103", "11", false),
            Person(
                    "2", "2", "f", "m", "e",
                    PersonProto.PersonValue.Gradering.UGRADERT,
                    listOf("t1", "t2"), "1101", "11", false),
            Person(
                    "2", "2", "f", "m", "e",
                    PersonProto.PersonValue.Gradering.UGRADERT,
                    listOf("t1", "t2"), "1102", "11", true),
            Person(
                    "2", "2", "f", "m", "e",
                    PersonProto.PersonValue.Gradering.UGRADERT,
                    listOf("t1", "t2"), "5402", "54", false),
            Person(
                    "2", "2", "f", "m", "e",
                    PersonProto.PersonValue.Gradering.UGRADERT,
                    listOf("t1", "t2"), "5401", "54", false)
    )

    private val filteredPersons = listOf(
            Person(
                    "1", "1", "f", "m", "e",
                    PersonProto.PersonValue.Gradering.UGRADERT,
                    listOf("t1", "t2"), "0301", "03", false),
            Person(
                    "2", "2", "f", "m", "e",
                    PersonProto.PersonValue.Gradering.UGRADERT,
                    listOf("t1", "t2"), "1101", "11", false),
            Person(
                    "2", "2", "f", "m", "e",
                    PersonProto.PersonValue.Gradering.UGRADERT,
                    listOf("t1", "t2"), "5401", "54", false)
    )

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

        "FilterPerson should be loaded as expected" {

            FilterPerson.fromJson(
                    Vault.getSecretOrDefault(
                    "personFilter",
                    p = "${System.getProperty("user.dir")}$vaultPath")
            ) shouldBe FilterPerson(regions = listOf(
                    Region(region = "03", municipals = emptyList()),
                    Region(region = "11", municipals = listOf("1101", "1102")),
                    Region(region = "54", municipals = listOf("5401"))
            ))

            FilterPerson.fromJson(Vault.getSecretOrDefault("personFilter"))
                    .shouldBeInstanceOf<FilterPersonInvalid>()
        }

        "fun applyFilter should work as expected" {

            val filter = FilterPerson.fromJson(
                    Vault.getSecretOrDefault(
                            "personFilter",
                            p = "${System.getProperty("user.dir")}$vaultPath")
            )

            persons.applyFilter(filter) shouldBe filteredPersons

            persons.applyFilter(FilterPerson(regions = emptyList())) shouldBe emptyList()
        }
    }
}
