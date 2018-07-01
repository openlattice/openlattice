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
import com.openlattice.data.DataEdge
import com.openlattice.data.EntityDataKey
import com.openlattice.data.requests.EntitySetSelection
import com.openlattice.data.requests.FileType
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.DataTables
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.apache.commons.lang3.RandomStringUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
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
        waitForIt()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)
        waitForIt()

        val testData = TestDataFactory
                .randomStringEntityData(numberOfEntries, et.properties)
        MultipleAuthenticatedUsersBase.dataApi.replaceEntities(es.id, testData, false)
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
        waitForIt()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)
        waitForIt()

        val testData = TestDataFactory.randomStringEntityData(numberOfEntries, et.properties)

        val entries = ImmutableList.copyOf(testData.values)
        val ids = dataApi.createOrMergeEntities(es.id, entries)

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

        val replacementMap = mapOf(ids[0]!! to replacement)

        Assert.assertEquals(1, dataApi.replaceEntities(es.id, replacementMap, true))

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
        waitForIt()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)
        waitForIt()
        val src = MultipleAuthenticatedUsersBase.createEntityType()
        waitForIt()
        val esSrc = MultipleAuthenticatedUsersBase.createEntitySet(src)
        waitForIt()
        val dst = MultipleAuthenticatedUsersBase.createEntityType()
        waitForIt()
        val esDst = MultipleAuthenticatedUsersBase.createEntitySet(dst)
        waitForIt()
        val at = MultipleAuthenticatedUsersBase.createAssociationType(et, setOf(src), setOf(dst))
        waitForIt()

        val testDataSrc = TestDataFactory.randomStringEntityData(numberOfEntries, src.properties)
        val testDataDst = TestDataFactory.randomStringEntityData(numberOfEntries, dst.properties)

        val entriesSrc = ImmutableList.copyOf(testDataSrc.values)
        val idsSrc = dataApi.createOrMergeEntities(esSrc.id, entriesSrc)

        val entriesDst = ImmutableList.copyOf(testDataDst.values)
        val idsDst = dataApi.createOrMergeEntities(esDst.id, entriesDst)

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

        Assert.assertEquals(  edgeData.second.map{ it.data } , actualEdgeData )
        dataApi.ass
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

    private fun waitForIt() {
        try {
            Thread.sleep(1000)
        } catch (e: InterruptedException) {
            throw IllegalStateException("Failed to wait for it.")
        }

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

        waitForIt()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)
        waitForIt()

        val testData = HashMap<UUID, SetMultimap<UUID, Any>>()
        val d = LocalDate.now()
        val odt = OffsetDateTime.ofInstant(Instant.now(), ZoneId.of("UTC"))
        testData.put(
                UUID.randomUUID(),
                ImmutableSetMultimap
                        .of(p1.id, odt, p2.id, d, k.id, RandomStringUtils.randomAlphanumeric(5))
        )
        MultipleAuthenticatedUsersBase.dataApi.replaceEntities(es.id, testData, false)
        val ess = EntitySetSelection(Optional.of(et.properties))
        val results = Sets.newHashSet(
                MultipleAuthenticatedUsersBase.dataApi
                        .loadEntitySetData(es.id, ess, FileType.json)
        )

        Assert.assertEquals(testData.size.toLong(), results.size.toLong())
        val result = results.iterator().next()
        val p1v = OffsetDateTime.parse(result.get(p1.type).iterator().next() as CharSequence)
        val p2v = LocalDate.parse(result.get(p2.type).iterator().next() as CharSequence)

        Assert.assertEquals(odt, p1v)
        Assert.assertEquals(d, p2v)
    }

    @Test
    fun testLoadSelectedEntityData() {
        val et = MultipleAuthenticatedUsersBase.createEntityType()
        waitForIt()
        val es = MultipleAuthenticatedUsersBase.createEntitySet(et)
        waitForIt()

        val entities = TestDataFactory.randomStringEntityData(
                numberOfEntries,
                et.properties
        )
        MultipleAuthenticatedUsersBase.dataApi.replaceEntities(es.id, entities, false)

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
}