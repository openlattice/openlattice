package com.openlattice.rehearsal.linking

import com.openlattice.edm.type.EntityType
import com.openlattice.rehearsal.SetupTestData
import com.openlattice.rehearsal.edm.PERSON_NAME
import com.openlattice.rehearsal.edm.PERSON_NAMESPACE
import org.junit.AfterClass
import org.junit.BeforeClass
import java.lang.reflect.UndeclaredThrowableException

class LinkingFeedbackTest : SetupTestData() {
    private val numberOfEntries = 10

    companion object {
        private val importedEntitySets = mapOf(
                "SocratesTestA" to Pair("socratesA.yaml", "test_linked_person_1.csv"),
                "SocratesTestB" to Pair("socratesB.yaml", "test_linked_person_2.csv"),
                "SocratesTestC" to Pair("socratesC.yaml", "test_linked_person_3.csv"),
                "SocratesTestD" to Pair("socratesD.yaml", "test_not_linked_person_1.csv"))

        lateinit var personEt: EntityType

        @JvmStatic
        @BeforeClass
        fun init() {
            importedEntitySets.forEach {
                importDataSet(it.value.first, it.value.second)
            }

            Thread.sleep(10000L)
            while (!checkLinkingFinished(importedEntitySets.keys)) {
                Thread.sleep(5000L)
            }

            loginAs("admin")
            personEt = edmApi.getEntityType(edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME))
        }

        @JvmStatic
        @AfterClass
        fun tearDown() {
            importedEntitySets.keys.forEach {
                try {
                    edmApi.deleteEntitySet(edmApi.getEntitySetId(it))
                } catch (e: UndeclaredThrowableException) {
                }
            }
        }
    }


}