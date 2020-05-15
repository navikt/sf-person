package no.nav.sf.person

import io.kotest.assertions.asClue
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf

class SalesforceDSLTests : StringSpec() {

    init {

        "fun authorize should work with correct settings" {

            // Params defaults to MOCK
            Params().getSalesforceDetails().asClue { sf ->
                sf.configIsOk() shouldBe true
                sfMockAPI { sf.authorize().shouldBeInstanceOf<Authorization>() }
            }
        }

        "fun authorize should not work with invalid settings" {

            Params().getSalesforceDetails().copy(clientID = "invalid").asClue { sf ->
                sf.configIsOk() shouldBe true
                sfMockAPI { sf.authorize().shouldBeInstanceOf<AuthorizationMissing>() }
            }

            Params().getSalesforceDetails().copy(username = "invalid").asClue { sf ->
                sf.configIsOk() shouldBe true
                sfMockAPI { sf.authorize().shouldBeInstanceOf<AuthorizationMissing>() }
            }
        }

        "fun getSalesforceSObjectPostFun should not execute doSomething when failed authorization" {

            Params().getSalesforceDetails().copy(clientID = "invalid").asClue { sf ->
                sf.configIsOk() shouldBe true
                sfMockAPI {
                    getSalesforceSObjectPostFun(sf, AuthorizationMissing) {
                        false shouldBe true
                    }.shouldBeInstanceOf<AuthorizationMissing>()
                }
            }

            Params().getSalesforceDetails().copy(username = "invalid").asClue { sf ->
                sf.configIsOk() shouldBe true
                sfMockAPI {
                    getSalesforceSObjectPostFun(sf, AuthorizationMissing) {
                        false shouldBe true
                    }.shouldBeInstanceOf<AuthorizationMissing>()
                }
            }
        }

        "fun getSalesforceSObjectPostFun should execute doSomething when authorized" {

            Params().getSalesforceDetails().asClue { sf ->
                sf.configIsOk() shouldBe true
                sfMockAPI {
                    getSalesforceSObjectPostFun(sf, sf.authorize()) { sfPost ->
                        sfPost("some") shouldBe true // mock don't care about payload
                    }.shouldBeInstanceOf<Authorization>()
                }
            }
        }

        "fun sfPost should automatically re-authorize in case of expired token" {

            Params().getSalesforceDetails().asClue { sf ->
                sf.configIsOk() shouldBe true
                sfMockAPI {
                    getSalesforceSObjectPostFun(sf, AuthorizationMissing) { sfPost ->
                        sfPost("1st post") shouldBe true
                        sfPost("2nd post") shouldBe true
                    }.shouldBeInstanceOf<Authorization>()
                }
            }
        }

        "fun sfPost should automatically do 3 re-authorize attempts in case of expired token" {

            Params().getSalesforceDetails().asClue { sf ->
                sf.configIsOk() shouldBe true
                sfMockAPI {
                    getSalesforceSObjectPostFun(sf, AuthorizationMissing) { sfPost ->
                        sfPost("1st post") shouldBe true
                        sfPost("2nd post") shouldBe true // due to re-post will give 3rd post
                        sfPost("4th post") shouldBe false
                    }.shouldBeInstanceOf<Authorization>()
                }
            }
        }

        "data class Salesforce for SCRATCH should work as expected".config(enabled = false) {

            Params(vault = Vault(), envVar = EnvVar(sfInstanceType = "SCRATCH")).getSalesforceDetails().asClue { sf ->
                sf.configIsOk() shouldBe true

                getSalesforceSObjectPostFun(sf, sf.authorize()) { sfPost ->
                    sfPost("disregards") shouldBe false // Bad request due to invalid json
                }.shouldBeInstanceOf<Authorization>()
            }
        }

        "data class Salesforce for PREPROD should work as expected".config(enabled = false) {

            Params(vault = Vault(), envVar = EnvVar(sfInstanceType = "PREPROD")).getSalesforceDetails().asClue { sf ->
                sf.configIsOk() shouldBe true

                getSalesforceSObjectPostFun(sf, sf.authorize()) { sfPost ->
                    sfPost("disregards") shouldBe false // Bad request due to invalid json
                }.shouldBeInstanceOf<Authorization>()
            }
        }
    }
}
