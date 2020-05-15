package no.nav.sf.person

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.http4k.client.ApacheClient
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Status

class NaisDSLTests : StringSpec({

    val endpoints = listOf(NAIS_ISALIVE, NAIS_ISREADY, NAIS_METRICS, NAIS_PRESTOP)

    "NaisDSL should give available Nais API at 8080 inside enable-context".config(enabled = true) {

        enableNAISAPI {
            endpoints.fold(true) { acc, ep ->
                acc && ApacheClient().invoke(Request(Method.GET, "$NAIS_URL$ep")).status == Status.OK
            } shouldBe true
        }

        enableNAISAPI {
            endpoints.fold(true) { acc, ep ->
                acc && ApacheClient().invoke(Request(Method.GET, "$NAIS_URL$ep")).status == Status.OK
            } shouldBe true
        }
    }

    "NaisDSL should not give available Nais API at 8080 outside enable-context" {

        enableNAISAPI { }

        endpoints.fold(true) { acc, ep ->
            acc && ApacheClient().invoke(Request(Method.GET, "$NAIS_URL$ep")).status == Status.CONNECTION_REFUSED
        } shouldBe true
    }
})
