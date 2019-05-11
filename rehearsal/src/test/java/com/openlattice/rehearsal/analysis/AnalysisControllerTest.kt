package com.openlattice.rehearsal.analysis

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ListMultimap
import com.openlattice.data.DataEdge
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import com.openlattice.rehearsal.edm.EdmTestConstants
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.util.*


class AnalysisControllerTest : MultipleAuthenticatedUsersBase() {
    companion object {
        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")
        }
    }

    @Test
    fun testGetNeighborTypesOnLinkingEntitySet() {
        val numberOfEntries = 10

        // Create linking entityset and add person entityset to it
        val personEntityTypeId = EdmTestConstants.personEt.id
        val personEt = edmApi.getEntityType( personEntityTypeId )
        val personEs = createEntitySet( personEt )

        val pt = createPropertyType()
        val linkingEs = createEntitySet( EdmTestConstants.personEt, true, setOf(personEs.id) )

        // Create edge and src entitysets (linked entity set is dst)
        val edge = MultipleAuthenticatedUsersBase.createEdgeEntityType()
        val esEdge = MultipleAuthenticatedUsersBase.createEntitySet(edge)
        val src = MultipleAuthenticatedUsersBase.createEntityType()
        val esSrc = MultipleAuthenticatedUsersBase.createEntitySet(src)
        MultipleAuthenticatedUsersBase.createAssociationType(edge, setOf(src), setOf(personEt))

        // Create entries for src, dst
        val testDataSrc = TestDataFactory.randomStringEntityData(numberOfEntries, src.properties)
        val testDataDst = TestDataFactory.randomStringEntityData(
                numberOfEntries,
                personEt.properties
                        .map { edmApi.getPropertyType( it ) }
                        .filter { it.datatype == EdmPrimitiveTypeKind.String }
                        .map { it.id }
                        .toSet() )


        val entriesSrc = testDataSrc.values.toList()
        val idsSrc = dataApi.createEntities(esSrc.id, entriesSrc)
        val entriesDst = testDataDst.values.toList()
        val idsDst = dataApi.createEntities(personEs.id, entriesDst)

        val edgesToBeCreated: ListMultimap<UUID, DataEdge> = ArrayListMultimap.create()
        val edgeData = createDataEdges(esEdge.id, esSrc.id, personEs.id, edge.properties, idsSrc, idsDst, numberOfEntries)
        edgesToBeCreated.putAll(edgeData.first, edgeData.second)
        dataApi.createAssociations(edgesToBeCreated)

        val neighborTypes = analysisApi.getNeighborTypes( linkingEs.id )

        Assert.assertEquals( 1, neighborTypes.count() )
        neighborTypes.forEach {
            Assert.assertEquals( edge , it.associationEntityType )
            Assert.assertEquals( src, it.neighborEntityType )
        }
    }
}