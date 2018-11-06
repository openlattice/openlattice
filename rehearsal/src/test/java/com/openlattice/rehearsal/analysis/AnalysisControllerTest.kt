package com.openlattice.rehearsal.analysis

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ImmutableList
import com.google.common.collect.ListMultimap
import com.openlattice.data.DataEdge
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.apache.olingo.commons.api.edm.EdmPrimitiveType
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.LoggerFactory
import java.util.*


class AnalysisControllerTest : MultipleAuthenticatedUsersBase() {
    companion object {
        val logger = LoggerFactory.getLogger(AnalysisControllerTest::class.java)

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
        val personEntityTypeId = edmApi.getEntityTypeId( PERSON_NAMESPACE, PERSON_NAME )
        val personEt = edmApi.getEntityType( personEntityTypeId )
        val personEs = createEntitySet( personEt )

        val pt = createPropertyType()
        val et = createEntityType( pt.id )
        val linkingEs = createEntitySet( et, true, setOf(personEs.id) )

        // Create edge and src entitysets (linked entity set is dst)
        val edge = MultipleAuthenticatedUsersBase.createEdgeEntityType()
        val esEdge = MultipleAuthenticatedUsersBase.createEntitySet(edge)
        val src = MultipleAuthenticatedUsersBase.createEntityType()
        val esSrc = MultipleAuthenticatedUsersBase.createEntitySet(src)
        val at = MultipleAuthenticatedUsersBase.createAssociationType(edge, setOf(src), setOf(personEt))

        // Create entries for src, dst
        val testDataSrc = TestDataFactory.randomStringEntityData(numberOfEntries, src.properties)
        val testDataDst = TestDataFactory.randomStringEntityData(
                numberOfEntries,
                personEt.properties
                        .map { edmApi.getPropertyType( it ) }
                        .filter { it.datatype == EdmPrimitiveTypeKind.String }
                        .map { it.id }
                        .toSet() )


        val entriesSrc = ImmutableList.copyOf(testDataSrc.values)
        val idsSrc = dataApi.createEntities(esSrc.id, entriesSrc)
        val entriesDst = ImmutableList.copyOf(testDataDst.values)
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