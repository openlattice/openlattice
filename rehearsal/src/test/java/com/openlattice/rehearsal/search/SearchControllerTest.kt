package com.openlattice.rehearsal.search

import com.google.common.base.Strings
import com.google.common.collect.ImmutableList
import com.openlattice.authorization.*
import com.openlattice.data.DataEdgeKey
import com.openlattice.data.DeleteType
import com.openlattice.data.EntityDataKey
import com.openlattice.edm.EdmConstants
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import com.openlattice.search.requests.*
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.time.OffsetDateTime
import java.util.*

class SearchControllerTest : MultipleAuthenticatedUsersBase() {
    companion object {
        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")
        }
    }

    @Test
    fun testOrganizationIndex() {
        loginAs("admin")

        val organization = createOrganization()
        val id = organization.id
        Thread.sleep(3000)

        val search1 = searchApi.executeOrganizationSearch(
                SearchTerm(organization.title, 0, 10, Optional.empty<Boolean>()))
        Assert.assertTrue(search1.hits.mapTo(hashSetOf()) { it["id"] }.contains(id.toString()))

        val newDescription = Strings.repeat(organization.description, 2)
        organizationsApi.updateDescription(id, newDescription)
        Thread.sleep(3000)

        val search2 = searchApi.executeOrganizationSearch(
                SearchTerm(newDescription, 0, 10, Optional.empty<Boolean>()))
        Assert.assertTrue(search2.hits.mapTo(hashSetOf()) { it["id"] }.contains(id.toString()))

        searchApi.triggerOrganizationIndex(id)
        searchApi.triggerAllOrganizationsIndex()

        organizationsApi.destroyOrganization(id)
        val search3 = searchApi.executeOrganizationSearch(
                SearchTerm(organization.title, 0, 10, Optional.empty<Boolean>()))
        Assert.assertEquals(0, search3.hits.size)
    }

    @Test
    fun testEntitySetDataSearchAuthorizations() {
        /* Testing authorization checks for
         - searchEntitySetData, executeEntitySetDataQuery, executeAdvancedEntitySetDataQuery
         - executeEntityNeighborSearch, executeFilteredEntityNeighborSearch, executeFilteredEntityNeighborIdsSearch */

        // create data with admin
        val et = createEntityType()
        val es = createEntitySet(et)
        val dst = createEntitySet(et)
        val edgeEt = createEdgeEntityType()
        val edge = createEntitySet(edgeEt)
        createAssociationType(edgeEt, setOf(et), setOf(et))

        val pt = et.properties.random()
        val propertyType = edmApi.getPropertyType(pt)
        val apt = edgeEt.properties.random()
        val associationPropertyType = edmApi.getPropertyType(apt)

        val numberOfEntries = 10
        val testData = TestDataFactory.randomStringEntityData(numberOfEntries, et.properties)
        val testDataDst = TestDataFactory.randomStringEntityData(numberOfEntries, et.properties)
        val testDataEdge = TestDataFactory.randomStringEntityData(numberOfEntries, edgeEt.properties)

        val entries = ImmutableList.copyOf(testData.values)
        val ids = dataApi.createEntities(es.id, entries)
        val id = ids.first()
        val entriesDst = ImmutableList.copyOf(testDataDst.values)
        val idsDst = dataApi.createEntities(dst.id, entriesDst)
        val entriesEdge = ImmutableList.copyOf(testDataEdge.values)
        val idsEdge = dataApi.createEntities(edge.id, entriesEdge)


        val edges = ids.mapIndexed { index, _ ->
            DataEdgeKey(
                    EntityDataKey(es.id, ids[index]),
                    EntityDataKey(dst.id, idsDst[index]),
                    EntityDataKey(edge.id, idsEdge[index])
            )
        }.toSet()
        dataApi.createEdges(edges)


        // setup basic search parameters
        val simpleSearchConstraint = SearchConstraints
                .simpleSearchConstraints(arrayOf(es.id), 0, 100, "*")
        val searchTerm = SearchTerm("*", 0, 10)
        val advancedSearchTerm = AdvancedSearch(
                listOf(SearchDetails("*", pt, false)), 0, 100)
        val neighborsFilter = EntityNeighborsFilter(ids.toSet())


        // try to read data with no permissions on it
        loginAs("user1")

        val noEntitySetData1 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(0L, noEntitySetData1.numHits)
        Assert.assertEquals(0, noEntitySetData1.hits.size)
        val noEntitySetData2 = searchApi.executeEntitySetDataQuery(es.id, searchTerm)
        Assert.assertEquals(0L, noEntitySetData2.numHits)
        Assert.assertEquals(0, noEntitySetData2.hits.size)
        val noEntitySetData3 = searchApi.executeAdvancedEntitySetDataQuery(es.id, advancedSearchTerm)
        Assert.assertEquals(0L, noEntitySetData3.numHits)
        Assert.assertEquals(0, noEntitySetData3.hits.size)

        val noNeighborData1 = searchApi.executeEntityNeighborSearch(es.id, id)
        Assert.assertEquals(0, noNeighborData1.size)
        val noNeighborData2 = searchApi.executeFilteredEntityNeighborSearch(es.id, neighborsFilter)
        Assert.assertEquals(0, noNeighborData2.size)
        val noNeighborData3 = searchApi.executeFilteredEntityNeighborIdsSearch(es.id, neighborsFilter)
        Assert.assertEquals(0, noNeighborData3.size)

        loginAs("admin")


        // try to read data with only permission on entityset but no properties
        val readPermission = EnumSet.of(Permission.READ)
        val esReadAcl = Acl(AclKey(es.id), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(esReadAcl, Action.ADD))
        val dstReadAcl = Acl(AclKey(dst.id), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(dstReadAcl, Action.ADD))
        val edgeReadAcl = Acl(AclKey(edge.id), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(edgeReadAcl, Action.ADD))

        loginAs("user1")

        val noEntitySetData4 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(0, noEntitySetData4.numHits.toInt())
        Assert.assertEquals(0, noEntitySetData4.hits.size)

        val noEntitySetData5 = searchApi.executeEntitySetDataQuery(es.id, searchTerm)
        Assert.assertEquals(0, noEntitySetData5.numHits.toInt())
        Assert.assertEquals(0, noEntitySetData5.hits.size)

        val noEntitySetData6 = searchApi.executeAdvancedEntitySetDataQuery(es.id, advancedSearchTerm)
        Assert.assertEquals(0, noEntitySetData6.numHits.toInt())
        Assert.assertEquals(0, noEntitySetData6.hits.size)

        val noNeighborData4 = searchApi.executeEntityNeighborSearch(es.id, id)
        Assert.assertEquals(1, noNeighborData4.size)
        Assert.assertEquals(setOf(EdmConstants.ID_FQN), noNeighborData4[0].associationDetails.keys)
        Assert.assertEquals(setOf(EdmConstants.ID_FQN), noNeighborData4[0].neighborDetails.get().keys)

        val noNeighborData5 = searchApi.executeFilteredEntityNeighborSearch(es.id, neighborsFilter)
        Assert.assertEquals(numberOfEntries, noNeighborData5.size)
        Assert.assertEquals(1, noNeighborData5[ids.random()]!!.size)
        Assert.assertEquals(setOf(EdmConstants.ID_FQN), noNeighborData5[ids.random()]!![0].associationDetails.keys)
        Assert.assertEquals(setOf(EdmConstants.ID_FQN), noNeighborData5[ids.random()]!![0].neighborDetails.get().keys)

        val noNeighborData6 = searchApi.executeFilteredEntityNeighborIdsSearch(es.id, neighborsFilter)
        Assert.assertEquals(numberOfEntries, noNeighborData6.size)
        Assert.assertEquals(1, noNeighborData6[ids.random()]!!.size)
        Assert.assertEquals(1, noNeighborData6[ids.random()]!![edge.id]!!.size())
        Assert.assertEquals(setOf(dst.id), noNeighborData6[ids.random()]!![edge.id]!!.keySet())

        loginAs("admin")


        // try to read with permission on pt
        val ptReadAcl1 = Acl(AclKey(es.id, pt), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(ptReadAcl1, Action.ADD))
        val ptReadAcl2 = Acl(AclKey(dst.id, pt), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(ptReadAcl2, Action.ADD))
        val aptReadAcl = Acl(AclKey(edge.id, apt), setOf(Ace(user1, readPermission, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(aptReadAcl, Action.ADD))

        loginAs("user1")

        val ptData1 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(numberOfEntries, ptData1.numHits.toInt())
        Assert.assertEquals(numberOfEntries, ptData1.hits.size)
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, EdmConstants.LAST_WRITE_FQN, propertyType.type),
                ptData1.hits[0].keys
        )

        val ptData2 = searchApi.executeEntitySetDataQuery(es.id, searchTerm)
        Assert.assertEquals(numberOfEntries, ptData2.numHits.toInt())
        Assert.assertEquals(numberOfEntries, ptData2.hits.size)
        Assert.assertEquals(setOf(EdmConstants.ID_FQN, EdmConstants.LAST_WRITE_FQN, propertyType.type), ptData2.hits[0].keys)

        val ptData3 = searchApi.executeAdvancedEntitySetDataQuery(es.id, advancedSearchTerm)
        Assert.assertEquals(numberOfEntries, ptData3.numHits.toInt())
        Assert.assertEquals(numberOfEntries, ptData3.hits.size)
        Assert.assertEquals(setOf(EdmConstants.ID_FQN, EdmConstants.LAST_WRITE_FQN, propertyType.type), ptData3.hits[0].keys)

        val neighborData1 = searchApi.executeEntityNeighborSearch(es.id, id)
        Assert.assertEquals(1, neighborData1.size)
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, associationPropertyType.type),
                neighborData1[0].associationDetails.keys
        )
        Assert.assertEquals(setOf(EdmConstants.ID_FQN, propertyType.type), neighborData1[0].neighborDetails.get().keys)

        val neighborData2 = searchApi.executeFilteredEntityNeighborSearch(es.id, neighborsFilter)
        Assert.assertEquals(numberOfEntries, neighborData2.size)
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, associationPropertyType.type),
                neighborData2[ids.random()]!![0].associationDetails.keys
        )
        Assert.assertEquals(
                setOf(EdmConstants.ID_FQN, propertyType.type),
                neighborData2[ids.random()]!![0].neighborDetails.get().keys
        )

        val neighborData3 = searchApi.executeFilteredEntityNeighborIdsSearch(es.id, neighborsFilter)
        Assert.assertEquals(numberOfEntries, neighborData3.size)
        Assert.assertEquals(1, neighborData3[ids.random()]!!.size)
        Assert.assertEquals(1, neighborData3[ids.random()]!![edge.id]!!.size())
        Assert.assertEquals(setOf(dst.id), neighborData3[ids.random()]!![edge.id]!!.keySet())

        loginAs("admin")


        // give read permission on all property types
        et.properties.forEach {
            permissionsApi.updateAcl(AclData(
                    Acl(AclKey(es.id, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                    Action.ADD))
            permissionsApi.updateAcl(AclData(
                    Acl(AclKey(dst.id, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                    Action.ADD))
        }

        edgeEt.properties.forEach {
            permissionsApi.updateAcl(AclData(
                    Acl(AclKey(edge.id, it), setOf(Ace(user1, readPermission, OffsetDateTime.MAX))),
                    Action.ADD))
        }

        loginAs("user1")

        val searchResult1 = searchApi.searchEntitySetData(simpleSearchConstraint)
        Assert.assertEquals(numberOfEntries, searchResult1.numHits.toInt())
        Assert.assertEquals(numberOfEntries, searchResult1.hits.size)
        Assert.assertEquals(
                et.properties.map { edmApi.getPropertyType(it).type }.toSet()
                        + setOf(EdmConstants.ID_FQN, EdmConstants.LAST_WRITE_FQN),
                searchResult1.hits[0].keys)

        val searchResult2 = searchApi.executeEntitySetDataQuery(es.id, searchTerm)
        Assert.assertEquals(numberOfEntries, searchResult2.numHits.toInt())
        Assert.assertEquals(numberOfEntries, searchResult2.hits.size)
        Assert.assertEquals(
                et.properties.map { edmApi.getPropertyType(it).type }.toSet()
                        + setOf(EdmConstants.ID_FQN, EdmConstants.LAST_WRITE_FQN),
                searchResult2.hits[0].keys)

        val searchResult3 = searchApi.executeAdvancedEntitySetDataQuery(es.id, advancedSearchTerm)
        Assert.assertEquals(numberOfEntries, searchResult3.numHits.toInt())
        Assert.assertEquals(numberOfEntries, searchResult3.hits.size)
        Assert.assertEquals(
                et.properties.map { edmApi.getPropertyType(it).type }.toSet()
                        + setOf(EdmConstants.ID_FQN, EdmConstants.LAST_WRITE_FQN),
                searchResult3.hits[0].keys)

        val neighborData4 = searchApi.executeEntityNeighborSearch(es.id, id)
        Assert.assertEquals(1, neighborData4.size)
        Assert.assertEquals(
                edgeEt.properties.map { edmApi.getPropertyType(it).type }.toSet() + setOf(EdmConstants.ID_FQN),
                neighborData4[0].associationDetails.keys)
        Assert.assertEquals(
                et.properties.map { edmApi.getPropertyType(it).type }.toSet() + setOf(EdmConstants.ID_FQN),
                neighborData4[0].neighborDetails.get().keys)

        val neighborData5 = searchApi.executeFilteredEntityNeighborSearch(es.id, neighborsFilter)
        Assert.assertEquals(numberOfEntries, neighborData5.size)
        Assert.assertEquals(
                edgeEt.properties.map { edmApi.getPropertyType(it).type }.toSet() + setOf(EdmConstants.ID_FQN),
                neighborData5[ids.random()]!![0].associationDetails.keys)
        Assert.assertEquals(
                et.properties.map { edmApi.getPropertyType(it).type }.toSet() + setOf(EdmConstants.ID_FQN),
                neighborData5[ids.random()]!![0].neighborDetails.get().keys)

        val neighborData6 = searchApi.executeFilteredEntityNeighborIdsSearch(es.id, neighborsFilter)
        Assert.assertEquals(numberOfEntries, neighborData6.size)
        Assert.assertEquals(1, neighborData6[ids.random()]!!.size)
        Assert.assertEquals(1, neighborData6[ids.random()]!![edge.id]!!.size())
        Assert.assertEquals(setOf(dst.id), neighborData6[ids.random()]!![edge.id]!!.keySet())

        loginAs("admin")
    }

    @Test
    fun testEntityDeletion() {
        // create 2 entities
        val et = createEntityType()
        val es = createEntitySet(et)

        val testData = TestDataFactory.randomStringEntityData(2, et.properties).values.toList()
        val entities = dataApi.createEntities(es.id, testData).toSet().zip(testData).toMap()

        // should be indexed automatically
        val id1 = entities.keys.first()
        val id2 = entities.keys.last()
        val searchAll = SearchTerm("*", 0, 10)
        var searchedEntities = searchApi.executeEntitySetDataQuery(es.id, searchAll)

        Assert.assertEquals(2, searchedEntities.numHits)
        searchedEntities.hits.forEach { entityData ->
            val id = UUID.fromString(entityData.getValue(EdmConstants.ID_FQN).first() as String)
            Assert.assertTrue(entities.keys.contains(id))
        }


        /* Hard delete */
        dataApi.deleteEntity(es.id, id1, DeleteType.Hard)

        // should be deleted automatically
        searchedEntities = searchApi.executeEntitySetDataQuery(es.id, searchAll)

        Assert.assertEquals(1, searchedEntities.numHits)
        Assert.assertEquals(
                id2,
                UUID.fromString(searchedEntities.hits.first().getValue(EdmConstants.ID_FQN).first() as String)
        )


        /* Soft delete */
        dataApi.deleteEntity(es.id, id2, DeleteType.Soft)

        // should be deleted automatically
        searchedEntities = searchApi.executeEntitySetDataQuery(es.id, searchAll)
        Assert.assertEquals(0, searchedEntities.numHits)
    }

    @Test
    fun testEntitySetDataDeletion() {
        // create 2 entity sets
        val et = createEntityType()
        val es1 = createEntitySet(et)
        val es2 = createEntitySet(et)

        // add data
        val testData = TestDataFactory.randomStringEntityData(1, et.properties).values.toList()
        val id1 = dataApi.createEntities(es1.id, testData).first()
        val id2 = dataApi.createEntities(es2.id, testData).first()

        // should be indexed automatically
        val searchAll = SearchTerm("*", 0, 10)
        var searchedEntities1 = searchApi.executeEntitySetDataQuery(es1.id, searchAll)
        var searchedEntities2 = searchApi.executeEntitySetDataQuery(es2.id, searchAll)

        Assert.assertEquals(1, searchedEntities1.numHits)
        Assert.assertEquals(1, searchedEntities2.numHits)
        Assert.assertEquals(
                id1,
                UUID.fromString(searchedEntities1.hits.first().getValue(EdmConstants.ID_FQN).first() as String)
        )
        Assert.assertEquals(
                id2,
                UUID.fromString(searchedEntities2.hits.first().getValue(EdmConstants.ID_FQN).first() as String)
        )

        /* Hard delete */
        dataApi.deleteAllEntitiesFromEntitySet(es1.id, DeleteType.Hard)

        // should be deleted automatically
        searchedEntities1 = searchApi.executeEntitySetDataQuery(es1.id, searchAll)
        searchedEntities2 = searchApi.executeEntitySetDataQuery(es2.id, searchAll)
        Assert.assertEquals(0, searchedEntities1.numHits)
        Assert.assertEquals(1, searchedEntities2.numHits)
        Assert.assertEquals(
                id2,
                UUID.fromString(searchedEntities2.hits.first().getValue(EdmConstants.ID_FQN).first() as String)
        )


        /* Soft delete */
        dataApi.deleteAllEntitiesFromEntitySet(es2.id, DeleteType.Soft)

        // should be deleted automatically
        searchedEntities1 = searchApi.executeEntitySetDataQuery(es1.id, searchAll)
        searchedEntities2 = searchApi.executeEntitySetDataQuery(es2.id, searchAll)
        Assert.assertEquals(0, searchedEntities1.numHits)
        Assert.assertEquals(0, searchedEntities2.numHits)
    }

}