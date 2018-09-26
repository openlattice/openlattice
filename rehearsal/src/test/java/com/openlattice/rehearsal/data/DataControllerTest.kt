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
import com.openlattice.data.DataEdge
import com.openlattice.data.EntityDataKey
import com.openlattice.data.UpdateType
import com.openlattice.data.requests.EntitySetSelection
import com.openlattice.data.requests.FileType
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.DataTables
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.apache.commons.lang3.RandomStringUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.LoggerFactory
import java.lang.Math.abs
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private val numberOfEntries = 10
private val random = Random()

class DataControllerTest : MultipleAuthenticatedUsersBase() {
    companion object {
        val logger = LoggerFactory.getLogger(DataControllerTest.javaClass)

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

        //added transformValues()
        val testData = transformValues(TestDataFactory.randomStringEntityData(numberOfEntries, et.properties), Multimaps::asMap)
        MultipleAuthenticatedUsersBase.dataApi.updateEntitiesInEntitySet(es.id, testData, UpdateType.Replace)
        val ess = EntitySetSelection(Optional.of(et.properties))
        val results = Sets.newHashSet(
                MultipleAuthenticatedUsersBase.dataApi
                        .loadEntitySetData(es.id, ess, FileType.json)
        )

        Assert.assertEquals(numberOfEntries.toLong(), results.size.toLong())
    }


    @Test
    fun testCreateAndLoadBinaryEntityData() {
        val pt = MultipleAuthenticatedUsersBase.getBinaryPropertyType()
        val et = MultipleAuthenticatedUsersBase.createEntityType(pt.id)
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)

        val testData = Maps.transformValues(TestDataFactory.randomBinaryData(numberOfEntries, et.key.iterator().next(),pt.id) ,Multimaps::asMap )
        MultipleAuthenticatedUsersBase.dataApi.updateEntitiesInEntitySet(es.id, testData, UpdateType.Replace)

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
        val data = ImmutableList
                .copyOf(dataApi.loadEntitySetData(es.id, ess, FileType.json))
        val indexActual = index(data)

        //Remove the extra properties for easier equals.
        indexActual.forEach {
            it.value.removeAll(DataTables.ID_FQN)
            it.value.removeAll(DataTables.LAST_INDEX_FQN)
            it.value.removeAll(DataTables.LAST_WRITE_FQN)
        }

        Assert.assertEquals(indexExpected, indexActual)

        val propertySrc = entries[0];
        val replacement: SetMultimap<UUID, Any> = HashMultimap.create()
        val replacementProperty = propertySrc.keySet().first();
        replacement.put(replacementProperty, RandomStringUtils.random(10) as Object)

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
                data2[0][fqnCache[replacementProperty]] == indexActual[ids[0]]!![fqnCache[replacementProperty]]
        )
    }

    @Test
    fun createEdges() {
        val et = MultipleAuthenticatedUsersBase.createEntityType()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)
        val src = MultipleAuthenticatedUsersBase.createEntityType()
        val esSrc = MultipleAuthenticatedUsersBase.createEntitySet(src)
        val dst = MultipleAuthenticatedUsersBase.createEntityType()
        val esDst = MultipleAuthenticatedUsersBase.createEntitySet(dst)
        val at = MultipleAuthenticatedUsersBase.createAssociationType(et, setOf(src), setOf(dst))

        val testDataSrc = TestDataFactory.randomStringEntityData(numberOfEntries, src.properties)
        val testDataDst = TestDataFactory.randomStringEntityData(numberOfEntries, dst.properties)

        val entriesSrc = ImmutableList.copyOf(testDataSrc.values)
        val idsSrc = dataApi.createEntities(esSrc.id, entriesSrc)

        val entriesDst = ImmutableList.copyOf(testDataDst.values)
        val idsDst = dataApi.createEntities(esDst.id, entriesDst)

        val indexExpectedSrc = entriesSrc.mapIndexed { index, data -> idsSrc[index] to keyByFqn(data) }.toMap()
        val indexExpectedDst = entriesSrc.mapIndexed { index, data -> idsDst[index] to keyByFqn(data) }.toMap()


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

        val actualEdgeData = ImmutableList.copyOf(dataApi.loadEntitySetData(es.id, ess, FileType.json))
        Multimaps.asMap(edgesToBeCreated).entries.first().value.
                mapIndexed { index, de ->
                   val edgeDataLookup = lookupEdgeDataByFqn(actualEdgeData[index].asMap())
                    de.data.asMap().
                        forEach { uuid, data -> Assert.assertEquals(data, edgeDataLookup[uuid]) }
                }
    }

    private fun lookupEdgeDataByFqn(edgeData: MutableMap<FullQualifiedName, MutableCollection<Any>>):
            Map<UUID, MutableCollection<Any>> {
        return edgeData.mapKeys { entry -> edmApi.getPropertyTypeId(entry.key.namespace, entry.key.name) }
    }


    private fun createDataEdges(
            entitySetId: UUID,
            properties: Set<UUID>,
            srcIds: List<UUID>,
            dstIds: List<UUID>
    ): Pair<UUID, List<DataEdge>> {
        val edgeData = TestDataFactory.randomStringEntityData(numberOfEntries, properties).values.toList()

        val edges = srcIds.mapIndexed { index, data ->
            val srcDataKey = EntityDataKey(entitySetId, srcIds[index])
            val dstDataKey = EntityDataKey(entitySetId, dstIds[index])
            DataEdge(srcDataKey, dstDataKey, edgeData[index])
        }

        return entitySetId to edges;
    }

    private fun keyByFqn(data: SetMultimap<UUID, Any>): SetMultimap<FullQualifiedName, Any> {
        val rekeyed: SetMultimap<FullQualifiedName, Any> = HashMultimap.create()
        Multimaps.asMap(data)
                .forEach { rekeyed.putAll(fqnCache[it.key], it.value) }
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
        val testData = Arrays.asList(HashMultimap.create(ImmutableSetMultimap
                        .of(p1.id, odt, p2.id, d, k.id, RandomStringUtils.randomAlphanumeric(5))) as SetMultimap<UUID, Any>)

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
        val odtMillisec = odt.nano/1000000
        val p1vMillisec = p1v.nano/1000000
        Assert.assertTrue(abs(odtMillisec - p1vMillisec) < 100)
        Assert.assertEquals(d, p2v)
    }

    @Test
    fun testLoadSelectedEntityData() {
        val et = MultipleAuthenticatedUsersBase.createEntityType()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)

        //added transformValues()
        val entities = TestDataFactory.randomStringEntityData(
                numberOfEntries,
                et.properties
        )
        MultipleAuthenticatedUsersBase.dataApi.updateEntitiesInEntitySet(es.id, transformValues(entities, Multimaps::asMap), UpdateType.Replace)

        // load selected data
        val selectedProperties = et.properties.asSequence()
                .filter { pid -> random.nextBoolean() }
                .toSet()
        val ess = EntitySetSelection(Optional.of(selectedProperties))
        val results = MultipleAuthenticatedUsersBase.dataApi.loadEntitySetData(es.id, ess, null)

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
        for (entity in entities.values) {
            expectedValues
                    .add(
                            entity.asMap().entries
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
        val testData = transformValues(TestDataFactory.randomStringEntityData(1, et.properties), Multimaps::asMap)
        MultipleAuthenticatedUsersBase.dataApi.updateEntitiesInEntitySet(es.id, testData, UpdateType.Replace)

        val oldNameSpace = pt.type.namespace
        val newNameSpace = oldNameSpace + "extrachars"

        // Update propertytype type
        val update = MetadataUpdate(Optional.of(pt.title), Optional.empty(), Optional.of(es.name),
                Optional.of(es.contacts), Optional.of(FullQualifiedName(newNameSpace, pt.type.name)), Optional.empty(),
                Optional.empty(), Optional.empty(),Optional.empty())
        edmApi.updatePropertyTypeMetadata(pt.id, update)

        val ess = EntitySetSelection(Optional.of(et.properties))
        val results = Sets.newHashSet(
                MultipleAuthenticatedUsersBase.dataApi
                        .loadEntitySetData(es.id, ess, FileType.json)
        )

        val fqns = results.iterator().next().keys()
        Assert.assertEquals(1, fqns.asSequence().filter { it.namespace.equals(newNameSpace) }.count())
        Assert.assertEquals(0, fqns.asSequence().filter { it.namespace.equals(oldNameSpace) }.count())
    }
}

