/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.rehearsal.data

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.collect.*
import com.google.common.collect.Maps.transformValues
import com.openlattice.authorization.*
import com.openlattice.data.*
import com.openlattice.data.requests.EntitySetSelection
import com.openlattice.data.requests.FileType
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.DataTables
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import com.openlattice.rehearsal.edm.*
import com.openlattice.search.requests.EntityNeighborsFilter
import com.openlattice.search.requests.SearchTerm
import org.apache.commons.lang.RandomStringUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import java.lang.Math.abs
import java.lang.reflect.UndeclaredThrowableException
import java.time.*
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private val numberOfEntries = 10
private val random = Random()
private val OL_ID_FQN = FullQualifiedName("openlattice", "@id")

class DataControllerTest : MultipleAuthenticatedUsersBase() {
    companion object {
        val fqnCache: LoadingCache<UUID, FullQualifiedName> = CacheBuilder.newBuilder()
                .build(
                        object : CacheLoader<UUID, FullQualifiedName>() {
                            override fun load(key: UUID?): FullQualifiedName {
                                return edmApi.getPropertyType(key!!).type
                            }

                        }
                )

        @JvmStatic
        @BeforeClass
        fun init() {
            loginAs("admin")
        }
    }

    @Test
    fun testCreateAndLoadEntityData() {
        val et = MultipleAuthenticatedUsersBase.createEntityType()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)

        val testData = TestDataFactory.randomStringEntityData(numberOfEntries, et.properties)
                .values
                .toList()
        val entities = dataApi.createEntities(es.id, testData).toSet().zip(testData).toMap()
        val ess = EntitySetSelection(Optional.of(et.properties))
        val results1 = Sets.newHashSet(dataApi.loadEntitySetData(es.id, ess, FileType.json))

        Assert.assertEquals(numberOfEntries.toLong(), results1.size.toLong())
        results1.forEach {
            val id = it[OL_ID_FQN].first()
            val originalData = entities.getValue(UUID.fromString(id as String))
            it.forEach { fqn, value ->
                if (fqn != OL_ID_FQN) {
                    val propertyId = edmApi.getPropertyTypeId(fqn.namespace, fqn.name)
                    Assert.assertEquals(originalData.getValue(propertyId).first(), value)
                }
            }
        }

        // optional/nullable EntitySetSelection in loadEntitySetData cannot be tested from here, only manually
        // Retrofit will throw java.lang.IllegalArgumentException: Body parameter value must not be null.
    }

    @Test
    fun testLoadEntityDataEmptySelection() {
        val et = MultipleAuthenticatedUsersBase.createEntityType()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)

        val testData = TestDataFactory.randomStringEntityData(numberOfEntries, et.properties)
                .values
                .toList()
        dataApi.createEntities(es.id, testData)
        val ess = EntitySetSelection(Optional.empty(), Optional.empty())
        val results = Sets.newHashSet(dataApi.loadEntitySetData(es.id, ess, FileType.json))

        Assert.assertEquals(numberOfEntries.toLong(), results.size.toLong())
    }


    @Test
    fun testCreateAndLoadBinaryEntityData() {
        val pt = MultipleAuthenticatedUsersBase.getBinaryPropertyType()
        val et = MultipleAuthenticatedUsersBase.createEntityType(pt.id)
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)

        val testData = ImmutableList.copyOf(
                TestDataFactory.randomBinaryData(numberOfEntries, et.key.iterator().next(), pt.id).values
        )
        MultipleAuthenticatedUsersBase.dataApi.createEntities(es.id, testData)

        val ess = EntitySetSelection(Optional.of(et.properties))
        val results = Sets.newHashSet(
                MultipleAuthenticatedUsersBase.dataApi
                        .loadEntitySetData(es.id, ess, FileType.json)
        )

        Assert.assertEquals(numberOfEntries.toLong(), results.size.toLong())
    }

    @Test
    fun testCreateLoadReplaceLoadData() {
        val et = MultipleAuthenticatedUsersBase.createEntityType()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)

        val testData = TestDataFactory.randomStringEntityData(numberOfEntries, et.properties)

        val entries = ImmutableList.copyOf(testData.values)
        val ids = dataApi.createEntities(es.id, entries)

        val indexExpected = entries.mapIndexed { index, data -> ids[index] to keyByFqn(data) }.toMap()

        val ess = EntitySetSelection(
                Optional.of(et.properties),
                Optional.of(HashSet(ids))
        )
        val data = ImmutableList.copyOf(dataApi.loadEntitySetData(es.id, ess, FileType.json))
        val indexActual = index(data)

        //Remove the extra properties for easier equals.
        indexActual.forEach {
            it.value.removeAll(DataTables.ID_FQN)
            it.value.removeAll(DataTables.LAST_INDEX_FQN)
            it.value.removeAll(DataTables.LAST_WRITE_FQN)
        }

        Assert.assertEquals(indexExpected, indexActual)

        val propertySrc = entries[0]
        val replacement: SetMultimap<UUID, Any> = HashMultimap.create()
        val replacementProperty = propertySrc.keys.first()
        replacement.put(replacementProperty, RandomStringUtils.random(10) as Any)

        //added transformValues()
        val replacementMap = transformValues(mapOf(ids[0]!! to replacement), Multimaps::asMap)

        Assert.assertEquals(1, dataApi.updateEntitiesInEntitySet(es.id, replacementMap, UpdateType.PartialReplace))

        val ess2 = EntitySetSelection(
                Optional.of(et.properties),
                Optional.of(setOf(ids[0]))
        )
        val data2 = ImmutableList
                .copyOf(dataApi.loadEntitySetData(es.id, ess2, FileType.json))

        val indexActual2 = index(data2)

        //Remove the extra properties for easier equals.
        indexActual2.forEach {
            it.value.removeAll(DataTables.ID_FQN)
            it.value.removeAll(DataTables.LAST_INDEX_FQN)
            it.value.removeAll(DataTables.LAST_WRITE_FQN)
        }

        Assert.assertFalse(
                data2[0][fqnCache[replacementProperty]] == indexActual.getValue(ids[0])[fqnCache[replacementProperty]]
        )
    }

    @Test
    fun createEdges() {
        val src = MultipleAuthenticatedUsersBase.createEntityType()
        val dst = MultipleAuthenticatedUsersBase.createEntityType()
        val edge = MultipleAuthenticatedUsersBase.createEdgeEntityType()

        val esSrc = MultipleAuthenticatedUsersBase.createEntitySet(src)
        val esDst = MultipleAuthenticatedUsersBase.createEntitySet(dst)
        val esEdge = MultipleAuthenticatedUsersBase.createEntitySet(edge)

        val testDataSrc = TestDataFactory.randomStringEntityData(numberOfEntries, src.properties)
        val testDataDst = TestDataFactory.randomStringEntityData(numberOfEntries, dst.properties)
        val testDataEdge = TestDataFactory.randomStringEntityData(numberOfEntries, edge.properties)

        val entriesSrc = ImmutableList.copyOf(testDataSrc.values)
        val idsSrc = dataApi.createEntities(esSrc.id, entriesSrc)

        val entriesDst = ImmutableList.copyOf(testDataDst.values)
        val idsDst = dataApi.createEntities(esDst.id, entriesDst)

        val entriesEdge = ImmutableList.copyOf(testDataEdge.values)
        val idsEdge = dataApi.createEntities(esEdge.id, entriesEdge)

        val edges = idsSrc.mapIndexed { index, _ ->
            DataEdgeKey(
                    EntityDataKey(esSrc.id, idsSrc[index]),
                    EntityDataKey(esDst.id, idsDst[index]),
                    EntityDataKey(esEdge.id, idsEdge[index])
            )
        }.toSet()
        val createdEdges = dataApi.createAssociations(edges)

        Assert.assertNotNull(createdEdges)
        Assert.assertEquals(edges.size, createdEdges)

        // Test permissions on property types. First add read permission on entity sets
        val add = EnumSet.of(Permission.READ)
        val newAcl1 = Acl(AclKey(esSrc.id), setOf(Ace(user1, add, OffsetDateTime.now(ZoneOffset.UTC))))
        val newAcl2 = Acl(AclKey(esDst.id), setOf(Ace(user1, add, OffsetDateTime.now(ZoneOffset.UTC))))
        val newAcl3 = Acl(AclKey(esEdge.id), setOf(Ace(user1, add, OffsetDateTime.now(ZoneOffset.UTC))))
        permissionsApi.updateAcl(AclData(newAcl1, Action.ADD))
        permissionsApi.updateAcl(AclData(newAcl2, Action.ADD))
        permissionsApi.updateAcl(AclData(newAcl3, Action.ADD))

        try {
            loginAs("user1")
            dataApi.createAssociations(edges)
            Assert.fail("Should have thrown Exception but did not!")
        } catch (e: UndeclaredThrowableException) {
            val uuidRegex = Regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
            val forbiddenAclKey = uuidRegex.findAll(e.undeclaredThrowable.message!!)
            Assert.assertEquals(esEdge.id, UUID.fromString(forbiddenAclKey.first().value))
            Assert.assertTrue(edge.key.contains(UUID.fromString(forbiddenAclKey.last().value)))
        } finally {
            loginAs("admin")
        }
    }

    @Test
    fun createEdgesWithData() {
        val et = MultipleAuthenticatedUsersBase.createEdgeEntityType()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)
        val src = MultipleAuthenticatedUsersBase.createEntityType()
        val esSrc = MultipleAuthenticatedUsersBase.createEntitySet(src)
        val dst = MultipleAuthenticatedUsersBase.createEntityType()
        val esDst = MultipleAuthenticatedUsersBase.createEntitySet(dst)

        val testDataSrc = TestDataFactory.randomStringEntityData(numberOfEntries, src.properties)
        val testDataDst = TestDataFactory.randomStringEntityData(numberOfEntries, dst.properties)

        val entriesSrc = ImmutableList.copyOf(testDataSrc.values)
        val idsSrc = dataApi.createEntities(esSrc.id, entriesSrc)

        val entriesDst = ImmutableList.copyOf(testDataDst.values)
        val idsDst = dataApi.createEntities(esDst.id, entriesDst)


        val edgesToBeCreated: ListMultimap<UUID, DataEdge> = ArrayListMultimap.create()
        val edgeData = createDataEdges(es.id, et.properties, idsSrc, idsDst)
        edgesToBeCreated.putAll(edgeData.first, edgeData.second)

        val createdEdges = dataApi.createAssociations(edgesToBeCreated)

        Assert.assertNotNull(createdEdges)
        Assert.assertEquals(edgeData.second.size, createdEdges.values().size)

        val ess = EntitySetSelection(
                Optional.of(et.properties),
                Optional.of(HashSet(createdEdges.values()))
        )

        // when loading entitysets, the result is grouped by entity key id
        val stringEntityKeyIds = createdEdges.values().map { it.toString() }

        val actualEdgeData = ImmutableList.copyOf(dataApi.loadEntitySetData(es.id, ess, FileType.json))
        val edgesCreatedData = Multimaps.asMap(edgesToBeCreated).entries.first().value
        actualEdgeData.mapIndexed { index, de ->
            val edgeDataLookup = lookupEdgeDataByFqn(
                    edgesCreatedData[numberOfEntries - index - 1].data.mapValues { it.value.toMutableSet() }.toMutableMap()
            )
            de.asMap()
                    .filter { it.key.name != OL_ID_FQN.name }
                    .forEach { fqn, data -> Assert.assertEquals(data, edgeDataLookup[fqn]) }
        }
    }

    private fun lookupEdgeDataByFqn(edgeData: MutableMap<UUID, MutableCollection<Any>>):
            Map<FullQualifiedName, MutableCollection<Any>> {
        return edgeData.mapKeys { entry -> edmApi.getPropertyType(entry.key).type }
    }


    private fun createDataEdges(
            entitySetId: UUID,
            properties: Set<UUID>,
            srcIds: List<UUID>,
            dstIds: List<UUID>
    ): Pair<UUID, List<DataEdge>> {
        val edgeData = ImmutableList.copyOf(TestDataFactory.randomStringEntityData(numberOfEntries, properties).values)

        val edges = srcIds.mapIndexed { index, data ->
            val srcDataKey = EntityDataKey(entitySetId, srcIds[index])
            val dstDataKey = EntityDataKey(entitySetId, dstIds[index])
            DataEdge(srcDataKey, dstDataKey, edgeData[index])
        }

        return entitySetId to edges
    }

    private fun keyByFqn(data: Map<UUID, Set<Any>>): SetMultimap<FullQualifiedName, Any> {
        val rekeyed: SetMultimap<FullQualifiedName, Any> = HashMultimap.create()
        data.forEach { rekeyed.putAll(fqnCache[it.key], it.value) }
        return rekeyed
    }

    private fun index(
            data: Collection<SetMultimap<FullQualifiedName, Any>>
    ): Map<UUID, SetMultimap<FullQualifiedName, Any>> {
        return data.map {
            UUID.fromString(it[DataTables.ID_FQN].first() as String) to it
        }.toMap()
    }

    @Test
    fun testDateTypes() {
        val p1 = MultipleAuthenticatedUsersBase.createDateTimePropertyType()
        val k = MultipleAuthenticatedUsersBase.createPropertyType()
        val p2 = MultipleAuthenticatedUsersBase.createDatePropertyType()

        val et = TestDataFactory.entityType(k)

        et.removePropertyTypes(et.properties)
        et.addPropertyTypes(ImmutableSet.of(k.id, p1.id, p2.id))

        val entityTypeId = MultipleAuthenticatedUsersBase.edmApi.createEntityType(et)
        Assert.assertNotNull("Entity type creation shouldn't return null UUID.", entityTypeId)

        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)

        val d = LocalDate.now()
        val odt = OffsetDateTime.ofInstant(Instant.now(), ZoneId.of("UTC"))
        val testData = Arrays.asList(
                Multimaps.asMap(
                        HashMultimap.create(
                                ImmutableSetMultimap
                                        .of(p1.id, odt, p2.id, d, k.id, RandomStringUtils.randomAlphanumeric(5))
                        ) as SetMultimap<UUID, Any>
                )
        )

        //added transformValues()
        val ids = MultipleAuthenticatedUsersBase.dataApi.createEntities(es.id, testData)
        val ess = EntitySetSelection(Optional.of(et.properties))
        val results = Sets.newHashSet(
                MultipleAuthenticatedUsersBase.dataApi
                        .loadEntitySetData(es.id, ess, FileType.json)
        )

        Assert.assertEquals(testData.size.toLong(), results.size.toLong())
        val result = results.iterator().next()
        val p1v = OffsetDateTime.parse(result.get(p1.type).iterator().next() as CharSequence)
        val p2v = LocalDate.parse(result.get(p2.type).iterator().next() as CharSequence)
        //There is a problem with the represenation of the DateTime of pv1, gets truncated. Instead code now
        //compares if odt and p1v are within 100 milliseconds
        val odtMillisec = odt.nano / 1000000
        val p1vMillisec = p1v.nano / 1000000
        Assert.assertTrue(abs(odtMillisec - p1vMillisec) < 100)
        Assert.assertEquals(d, p2v)
    }

    @Test
    fun testLoadSelectedEntityData() {
        val et = MultipleAuthenticatedUsersBase.createEntityType()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)

        //added transformValues()
        val entities = ImmutableList.copyOf(
                TestDataFactory.randomStringEntityData(numberOfEntries, et.properties).values
        )
        dataApi.createEntities(es.id, entities)

        // load selected data
        val selectedProperties = et.properties.asSequence()
                .filter { random.nextBoolean() }
                .toSet()
        val ess = EntitySetSelection(Optional.of(selectedProperties))
        val results = dataApi.loadEntitySetData(es.id, ess, null)

        // check results
        // For each entity, collect its property value in one set, and collect all these sets together.
        val resultValues = HashSet<Set<String>>()
        for (entity in results) {
            resultValues.add(
                    entity.asMap().entries.asSequence()
                            .filter { e -> !e.key.fullQualifiedNameAsString.contains("@") }
                            .flatMap { e -> e.value.asSequence() }
                            .map { o -> o as String }
                            .toSet()
            )
        }

        val expectedValues = HashSet<Set<String>>()
        for (entity in entities) {
            expectedValues
                    .add(
                            entity.entries
                                    .asSequence()
                                    // filter the entries with key (propertyId) in the selected set
                                    .filter { e ->
                                        selectedProperties.isEmpty() || selectedProperties
                                                .contains(e.key)
                                    }
                                    // Put all the property values in the same stream, and cast them back to strings
                                    .flatMap { e -> e.value.asSequence() }
                                    .map { o -> o as String }
                                    .toSet()
                    )
        }

        Assert.assertEquals(expectedValues, resultValues)
    }

    @Test
    fun testUpdatePropertyTypeMetadata() {
        val pt = createPropertyType()
        val et = createEntityType(pt.id)
        val es = createEntitySet(et)

        // add test data
        val testData = ImmutableList.copyOf(
                TestDataFactory.randomStringEntityData(1, et.properties).values
        )
        MultipleAuthenticatedUsersBase.dataApi.createEntities(es.id, testData)

        val oldNameSpace = pt.type.namespace
        val newNameSpace = oldNameSpace + "extrachars"

        // Update propertytype type
        val update = MetadataUpdate(
                Optional.of(pt.title), Optional.empty(), Optional.of(es.name),
                Optional.of(es.contacts), Optional.of(FullQualifiedName(newNameSpace, pt.type.name)), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()
        )
        edmApi.updatePropertyTypeMetadata(pt.id, update)

        val ess = EntitySetSelection(Optional.of(et.properties))
        val results = Sets.newHashSet(dataApi.loadEntitySetData(es.id, ess, FileType.json))

        val fqns = results.iterator().next().keys()
        Assert.assertEquals(1, fqns.asSequence().filter { it.namespace.equals(newNameSpace) }.count())
        Assert.assertEquals(0, fqns.asSequence().filter { it.namespace.equals(oldNameSpace) }.count())
    }


    /* Deletes */

    @Test
    fun testNotAuthorizedDelete() {
        val personEntityTypeId = edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME)
        val personEt = edmApi.getEntityType(personEntityTypeId)

        val es = createEntitySet(personEt)

        val personGivenNamePropertyId = edmApi.getPropertyTypeId(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)
        val entries = (1..numberOfEntries)
                .map { mapOf(personGivenNamePropertyId to setOf(RandomStringUtils.randomAscii(5))) }.toList()
        val newEntityIds = dataApi.createEntities(es.id, entries)

        // create edges with original entityset as source
        val dst = MultipleAuthenticatedUsersBase.createEntityType()
        val edge = MultipleAuthenticatedUsersBase.createEdgeEntityType()

        val esDst = MultipleAuthenticatedUsersBase.createEntitySet(dst)
        val esEdge = MultipleAuthenticatedUsersBase.createEntitySet(edge)

        val testDataDst = TestDataFactory.randomStringEntityData(numberOfEntries, dst.properties)
        val testDataEdge = TestDataFactory.randomStringEntityData(numberOfEntries, edge.properties)

        val entriesDst = ImmutableList.copyOf(testDataDst.values)
        val idsDst = dataApi.createEntities(esDst.id, entriesDst)

        val entriesEdge = ImmutableList.copyOf(testDataEdge.values)
        val idsEdge = dataApi.createEntities(esEdge.id, entriesEdge)

        val edges = newEntityIds.mapIndexed { index, _ ->
            DataEdgeKey(
                    EntityDataKey(es.id, newEntityIds[index]),
                    EntityDataKey(esDst.id, idsDst[index]),
                    EntityDataKey(esEdge.id, idsEdge[index])
            )
        }.toSet()
        dataApi.createAssociations(edges)


        /*   HARD DELETE   */

        try {
            loginAs("user1")
            dataApi.deleteEntities(es.id, newEntityIds.toSet(), DeleteType.Hard)
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Object [${es.id}] is not accessible.", true))
        } finally {
            loginAs("admin")
        }

        // add user1 as owner of entityset
        val ownerPermissions = EnumSet.of(Permission.OWNER)
        val esOwnerAcl = Acl(AclKey(es.id), setOf(Ace(user1, ownerPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(esOwnerAcl, Action.ADD))


        try {
            loginAs("user1")
            dataApi.deleteEntities(es.id, newEntityIds.toSet(), DeleteType.Hard)
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("You must have OWNER permission of all required entity set properties to delete entities from it.", true))
        } finally {
            loginAs("admin")
        }

        // add user1 as owner for all property types in entityset
        personEt.properties.forEach {
            val acl = Acl(AclKey(es.id, it), setOf(Ace(user1, ownerPermissions, OffsetDateTime.MAX)))
            permissionsApi.updateAcl(AclData(acl, Action.ADD))
        }

        try {
            loginAs("user1")
            dataApi.deleteEntities(es.id, newEntityIds.toSet(), DeleteType.Hard)
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Object [${esEdge.id}] is not accessible.", true))
        } finally {
            loginAs("admin")
        }

        // try to delete also neighbors
        // add user1 as owner of dst entity set
        val dstOwnerAcl = Acl(AclKey(esDst.id), setOf(Ace(user1, ownerPermissions, OffsetDateTime.MAX)))

        try {
            loginAs("user1")
            dataApi.deleteEntitiesAndNeighbors(
                    es.id,
                    EntityNeighborsFilter(
                            newEntityIds.toSet(),
                            Optional.empty(), Optional.of(setOf(esDst.id)), Optional.of(setOf(esEdge.id))),
                    DeleteType.Hard)
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Object [${esEdge.id}] is not accessible.", true))
        } finally {
            loginAs("admin")
        }

        // add user1 as owner of edge entity set
        val edgeOwnerAcl = Acl(AclKey(esEdge.id), setOf(Ace(user1, ownerPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(dstOwnerAcl, Action.ADD))
        permissionsApi.updateAcl(AclData(edgeOwnerAcl, Action.ADD))

        try {
            loginAs("user1")
            dataApi.deleteEntitiesAndNeighbors(
                    es.id,
                    EntityNeighborsFilter(
                            newEntityIds.toSet(),
                            Optional.empty(), Optional.of(setOf(esDst.id)), Optional.of(setOf(esEdge.id))),
                    DeleteType.Hard)
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("You must have OWNER permission of all required entity set properties to delete entities from it.", true))
        } finally {
            loginAs("admin")
        }


        /*   SOFT DELETE   */

        try {
            loginAs("user1")
            dataApi.deleteEntities(es.id, newEntityIds.toSet(), DeleteType.Soft)
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Object [${es.id}] is not accessible.", true))
        } finally {
            loginAs("admin")
        }

        // add read to user1 for entityset
        val readPermissions = EnumSet.of(Permission.READ)
        val esAcl = Acl(AclKey(es.id), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(esAcl, Action.ADD))


        try {
            loginAs("user1")
            dataApi.deleteEntities(es.id, newEntityIds.toSet(), DeleteType.Soft)
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("You must have WRITE permission of all required entity set properties to delete entities from it.", true))
        } finally {
            loginAs("admin")
        }

        // add write to user1 for all property types in entityset
        val writePermissions = EnumSet.of(Permission.WRITE)
        personEt.properties.forEach {
            val acl = Acl(AclKey(es.id, it), setOf(Ace(user1, writePermissions, OffsetDateTime.MAX)))
            permissionsApi.updateAcl(AclData(acl, Action.ADD))
        }

        try {
            loginAs("user1")
            dataApi.deleteEntities(es.id, newEntityIds.toSet(), DeleteType.Soft)
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Object [${esEdge.id}] is not accessible.", true))
        } finally {
            loginAs("admin")
        }

        // try to delete also neighbors
        try {
            loginAs("user1")
            dataApi.deleteEntitiesAndNeighbors(
                    es.id,
                    EntityNeighborsFilter(
                            newEntityIds.toSet(),
                            Optional.empty(), Optional.of(setOf(esDst.id)), Optional.of(setOf(esEdge.id))),
                    DeleteType.Soft)
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("Object [${esEdge.id}] is not accessible.", true))
        } finally {
            loginAs("admin")
        }

        // add read to user1 for dst and edge entity set
        val dstAcl = Acl(AclKey(esDst.id), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
        val edgeAcl = Acl(AclKey(esEdge.id), setOf(Ace(user1, readPermissions, OffsetDateTime.MAX)))
        permissionsApi.updateAcl(AclData(dstAcl, Action.ADD))
        permissionsApi.updateAcl(AclData(edgeAcl, Action.ADD))

        try {
            loginAs("user1")
            dataApi.deleteEntitiesAndNeighbors(
                    es.id,
                    EntityNeighborsFilter(
                            newEntityIds.toSet(),
                            Optional.empty(), Optional.of(setOf(esDst.id)), Optional.of(setOf(esEdge.id))),
                    DeleteType.Soft)
        } catch (e: UndeclaredThrowableException) {
            Assert.assertTrue(e.undeclaredThrowable.message!!
                    .contains("You must have WRITE permission of all required entity set properties to delete entities from it.", true))
        } finally {
            loginAs("admin")
        }

    }

    @Test
    fun testDeleteEntities() {
        val personEntityTypeId = edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME)
        val personEt = edmApi.getEntityType(personEntityTypeId)

        val es = createEntitySet(personEt)

        val personGivenNamePropertyId = edmApi.getPropertyTypeId(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)
        val entries = (1..numberOfEntries)
                .map { mapOf(personGivenNamePropertyId to setOf(RandomStringUtils.randomAscii(5))) }.toList()
        val newEntityIds = dataApi.createEntities(es.id, entries)

        Assert.assertEquals(numberOfEntries.toLong(), dataApi.getEntitySetSize(es.id))

        dataApi.deleteEntities(es.id, setOf(newEntityIds[0]), DeleteType.Hard)

        val ess = EntitySetSelection(Optional.of(personEt.properties))
        val loadedEntries = dataApi.loadEntitySetData(es.id, ess, FileType.json).toList()

        Assert.assertEquals(numberOfEntries - 1, loadedEntries.size)
        Assert.assertTrue(loadedEntries.none {
            it[FullQualifiedName(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)] == entries.first().values
        })
    }

    @Test
    fun testDeleteEntitiesWithAssociations() {
        val personEntityTypeId = edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME)
        val personEt = edmApi.getEntityType(personEntityTypeId)

        val es = createEntitySet(personEt)

        val personGivenNamePropertyId = edmApi.getPropertyTypeId(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)
        val entries = (1..numberOfEntries)
                .map { mapOf(personGivenNamePropertyId to setOf(RandomStringUtils.randomAscii(5))) }.toList()
        val newEntityIds = dataApi.createEntities(es.id, entries)

        // create edges with original entityset as source
        val dst = MultipleAuthenticatedUsersBase.createEntityType()
        val edge = MultipleAuthenticatedUsersBase.createEdgeEntityType()

        val esDst = MultipleAuthenticatedUsersBase.createEntitySet(dst)
        val esEdge = MultipleAuthenticatedUsersBase.createEntitySet(edge)

        val testDataDst = TestDataFactory.randomStringEntityData(numberOfEntries, dst.properties)
        val testDataEdge = TestDataFactory.randomStringEntityData(numberOfEntries, edge.properties)

        val entriesDst = ImmutableList.copyOf(testDataDst.values)
        val idsDst = dataApi.createEntities(esDst.id, entriesDst)

        val entriesEdge = ImmutableList.copyOf(testDataEdge.values)
        val idsEdge = dataApi.createEntities(esEdge.id, entriesEdge)

        val edges = newEntityIds.mapIndexed { index, _ ->
            DataEdgeKey(
                    EntityDataKey(es.id, newEntityIds[index]),
                    EntityDataKey(esDst.id, idsDst[index]),
                    EntityDataKey(esEdge.id, idsEdge[index])
            )
        }.toSet()
        dataApi.createAssociations(edges)


        // hard delete 1st entity
        dataApi.deleteEntities(es.id, setOf(newEntityIds[0]), DeleteType.Hard)

        val ess1 = EntitySetSelection(Optional.of(personEt.properties))
        val loadedEntries1 = dataApi.loadEntitySetData(es.id, ess1, FileType.json).toList()

        val essDst1 = EntitySetSelection(Optional.of(dst.properties))
        val loadedEntriesDst1 = dataApi.loadEntitySetData(esDst.id, essDst1, FileType.json).toList()

        val essEdge1 = EntitySetSelection(Optional.of(edge.properties))
        val loadedEntriesEdge1 = dataApi.loadEntitySetData(esEdge.id, essEdge1, FileType.json).toList()

        Assert.assertEquals(numberOfEntries - 1, loadedEntries1.size)
        Assert.assertEquals(numberOfEntries - 1, loadedEntriesEdge1.size)
        Assert.assertEquals(numberOfEntries, loadedEntriesDst1.size)
        Assert.assertTrue(loadedEntries1.none {
            it[FullQualifiedName(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)] == entries.first().values
        })
        Assert.assertTrue(loadedEntries1.none {
            it[OL_ID_FQN].first() == newEntityIds.first().toString()
        })
        Assert.assertTrue(loadedEntriesEdge1.none {
            it[OL_ID_FQN].first() == idsEdge.first().toString()
        })

        // soft delete last entity
        dataApi.deleteEntities(es.id, setOf(newEntityIds[numberOfEntries-1]), DeleteType.Soft)

        val ess2 = EntitySetSelection(Optional.of(personEt.properties))
        val loadedEntries2 = dataApi.loadEntitySetData(es.id, ess2, FileType.json).toList()

        val essDst2 = EntitySetSelection(Optional.of(dst.properties))
        val loadedEntriesDst = dataApi.loadEntitySetData(esDst.id, essDst2, FileType.json).toList()

        val essEdge2 = EntitySetSelection(Optional.of(edge.properties))
        val loadedEntriesEdge2 = dataApi.loadEntitySetData(esEdge.id, essEdge2, FileType.json).toList()

        Assert.assertEquals(numberOfEntries - 2, loadedEntries2.size)
        Assert.assertEquals(numberOfEntries - 2, loadedEntriesEdge2.size)
        Assert.assertEquals(numberOfEntries, loadedEntriesDst.size)
        Assert.assertTrue(loadedEntries2.none {
            it[FullQualifiedName(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)] == entries.last().values
        })
        Assert.assertTrue(loadedEntries2.none {
            it[OL_ID_FQN].last() == newEntityIds.last().toString()
        })
        Assert.assertTrue(loadedEntriesEdge2.none {
            it[OL_ID_FQN].last() == idsEdge.last().toString()
        })
    }

    @Test
    fun testDeleteAllDataInEntitySet() {
        // hard delete entityset data
        val personEntityTypeId = edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME)
        val personEt = edmApi.getEntityType(personEntityTypeId)

        val es = createEntitySet(personEt)

        val personGivenNamePropertyId = edmApi.getPropertyTypeId(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)
        val entries = (1..numberOfEntries)
                .map { mapOf(personGivenNamePropertyId to setOf(RandomStringUtils.randomAscii(5))) }.toList()
        val newEntityIds = dataApi.createEntities(es.id, entries)

        // create edges with original entityset as source
        val dst = MultipleAuthenticatedUsersBase.createEntityType()
        val edge = MultipleAuthenticatedUsersBase.createEdgeEntityType()

        val esDst = MultipleAuthenticatedUsersBase.createEntitySet(dst)
        val esEdge = MultipleAuthenticatedUsersBase.createEntitySet(edge)

        val testDataDst = TestDataFactory.randomStringEntityData(numberOfEntries, dst.properties)
        val testDataEdge = TestDataFactory.randomStringEntityData(numberOfEntries, edge.properties)

        val entriesDst = ImmutableList.copyOf(testDataDst.values)
        val idsDst = dataApi.createEntities(esDst.id, entriesDst)

        val entriesEdge = ImmutableList.copyOf(testDataEdge.values)
        val idsEdge = dataApi.createEntities(esEdge.id, entriesEdge)

        val edges = newEntityIds.mapIndexed { index, _ ->
            DataEdgeKey(
                    EntityDataKey(es.id, newEntityIds[index]),
                    EntityDataKey(esDst.id, idsDst[index]),
                    EntityDataKey(esEdge.id, idsEdge[index])
            )
        }.toSet()
        dataApi.createAssociations(edges)

        dataApi.deleteAllEntitiesFromEntitySet(es.id, DeleteType.Hard)

        val ess1 = EntitySetSelection(Optional.of(personEt.properties))
        val loadedEntries1 = dataApi.loadEntitySetData(es.id, ess1, FileType.json).toList()

        val essDst1 = EntitySetSelection(Optional.of(dst.properties))
        val loadedEntriesDst1 = dataApi.loadEntitySetData(esDst.id, essDst1, FileType.json).toList()

        val essEdge1 = EntitySetSelection(Optional.of(edge.properties))
        val loadedEntriesEdge1 = dataApi.loadEntitySetData(esEdge.id, essEdge1, FileType.json).toList()

        Assert.assertEquals(0, loadedEntries1.size)
        Assert.assertEquals(0, loadedEntriesEdge1.size)
        Assert.assertEquals(numberOfEntries, loadedEntriesDst1.size)

        Thread.sleep(5000L) // it takes some time to delete documents from elasticsearch
        Assert.assertEquals(0L, searchApi
                .executeEntitySetDataQuery(es.id, SearchTerm("*", 0, 10)).numHits)
        Assert.assertEquals(0L, searchApi
                .executeEntitySetDataQuery(esEdge.id, SearchTerm("*", 0, 10)).numHits)



        // soft delete entityset data
        val newEntityIds2 = dataApi.createEntities(es.id, entries)

        val idsEdge2 = dataApi.createEntities(esEdge.id, entriesEdge)
        val edges2 = newEntityIds.mapIndexed { index, _ ->
            DataEdgeKey(
                    EntityDataKey(es.id, newEntityIds2[index]),
                    EntityDataKey(esDst.id, idsDst[index]),
                    EntityDataKey(esEdge.id, idsEdge2[index])
            )
        }.toSet()
        dataApi.createAssociations(edges2)

        dataApi.deleteAllEntitiesFromEntitySet(es.id, DeleteType.Soft)

        val ess2 = EntitySetSelection(Optional.of(personEt.properties))
        val loadedEntries2 = dataApi.loadEntitySetData(es.id, ess2, FileType.json).toList()

        val essDst2 = EntitySetSelection(Optional.of(dst.properties))
        val loadedEntriesDst = dataApi.loadEntitySetData(esDst.id, essDst2, FileType.json).toList()

        val essEdge2 = EntitySetSelection(Optional.of(edge.properties))
        val loadedEntriesEdge2 = dataApi.loadEntitySetData(esEdge.id, essEdge2, FileType.json).toList()

        Assert.assertEquals(0, loadedEntries2.size)
        Assert.assertEquals(0, loadedEntriesEdge2.size)
        Assert.assertEquals(numberOfEntries, loadedEntriesDst.size)

        Thread.sleep(5000L) // it takes some time to delete documents from elasticsearch
        Assert.assertEquals(0L, searchApi
                .executeEntitySetDataQuery(es.id, SearchTerm("*", 0, 10)).numHits)
        Assert.assertEquals(0L, searchApi
                .executeEntitySetDataQuery(esEdge.id, SearchTerm("*", 0, 10)).numHits)
    }

    @Test
    fun testDeleteEntityProperties() {
        val personEntityTypeId = edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME)
        val personEt = edmApi.getEntityType(personEntityTypeId)

        val es = createEntitySet(personEt)

        val personGivenNamePropertyId = edmApi.getPropertyTypeId(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)
        val personMiddleNamePropertyId = edmApi.getPropertyTypeId(PERSON_MIDDLE_NAME_NAMESPACE, PERSON_MIDDLE_NAME_NAME)
        val people = (1..numberOfEntries)
                .map {
                    mapOf(
                            personGivenNamePropertyId to setOf(RandomStringUtils.randomAscii(5)),
                            personMiddleNamePropertyId to setOf(RandomStringUtils.randomAscii(5))
                    )
                }.toList()

        val newEntityIds = dataApi.createEntities(es.id, people)

        Assert.assertEquals(numberOfEntries.toLong(), dataApi.getEntitySetSize(es.id))

        val entityId = newEntityIds[0]
        dataApi.deleteEntityProperties(es.id, entityId, setOf(personGivenNamePropertyId), DeleteType.Hard)

        val loadedEntity = dataApi.getEntity(es.id, entityId)

        Assert.assertEquals(numberOfEntries.toLong(), dataApi.getEntitySetSize(es.id))
        Assert.assertFalse(
                loadedEntity.keySet()
                        .contains(FullQualifiedName(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME))
        )
    }
}
