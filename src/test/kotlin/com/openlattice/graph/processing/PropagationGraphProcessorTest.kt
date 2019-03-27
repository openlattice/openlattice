package com.openlattice.graph.processing

import com.google.common.collect.LinkedHashMultimap
import com.openlattice.analysis.requests.ValueFilter
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.EntityType
import com.openlattice.graph.processing.processors.*
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.mockito.Matchers
import org.mockito.Mockito
import org.mockito.Mockito.*
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.collections.LinkedHashSet

private const val entityType = "entity.type"
private const val start = "start.start"
private const val end = "end.end"
private const val duration = "duration.duration"

private const val assocType = "association.type"
private const val assocInput = "association.in"

class PropagationGraphProcessorTest {


    companion object {
        private val edm: EdmManager = mock(EdmManager::class.java)
        private val entitytTypeUUID = UUID.randomUUID()
        private val startUUID = UUID.randomUUID()
        private val endUUID = UUID.randomUUID()
        private val durationUUID = UUID.randomUUID()
        private val assocTypeUUID = UUID.randomUUID()
        private val assocInputUUID = UUID.randomUUID()

        private fun getUUIDFor(name: String): UUID {
            return when (name) {
                entityType -> entitytTypeUUID
                start -> startUUID
                end -> endUUID
                duration -> durationUUID
                assocType -> assocTypeUUID
                assocInput -> assocInputUUID
                else -> UUID.randomUUID()
            }
        }

        @BeforeClass
        @JvmStatic
        fun setupEdm() {

            val entityTypeAnswer = object : Answer<EntityType> {
                override fun answer(invocation: InvocationOnMock): EntityType {
                    val fqn = invocation.arguments[0] as FullQualifiedName
                    val uuid = getUUIDFor(fqn.fullQualifiedNameAsString)
                    return EntityType(uuid, fqn, "asd", Optional.of("asd"),
                            setOf<FullQualifiedName>(), LinkedHashSet<UUID>(), LinkedHashSet<UUID>(),
                            LinkedHashMultimap.create(), Optional.of(uuid), Optional.of(SecurableObjectType.EntityType), Optional.empty())
                }
            }

            val propertyIdAnswer = object : Answer<UUID> {
                override fun answer(invocation: InvocationOnMock): UUID {
                    val fqn = (invocation.arguments[0] as FullQualifiedName).fullQualifiedNameAsString
                    return getUUIDFor(fqn)
                }
            }

            Mockito.`when`(edm.getEntityType(Matchers.any(FullQualifiedName::class.java))).thenAnswer(entityTypeAnswer)
            Mockito.`when`(edm.getPropertyTypeId(Matchers.any(FullQualifiedName::class.java))).thenAnswer(propertyIdAnswer)

        }
    }

    @Test
    fun testHasCycle() {
        val durationProc = MockDurationProcessor()
        val endProc = MockEndDateProcessor()

        val propGraphProcessor = PropagationGraphProcessor(edm)
        propGraphProcessor.register(durationProc)
        propGraphProcessor.register(endProc)

        Assert.assertTrue(propGraphProcessor.hasCycle())
    }


    @Test
    fun testRootInputs() {
        val durationProc = MockDurationProcessor()
        val assocProc = MockAssociationProcessor()

        val propGraphProcessor = PropagationGraphProcessor(edm)
        propGraphProcessor.register(durationProc)
        propGraphProcessor.register(assocProc)

        Assert.assertEquals(
                propGraphProcessor.rootInputPropagations,
                setOf(Propagation(entitytTypeUUID, startUUID), Propagation(assocTypeUUID, assocInputUUID)))
    }

}

class MockDurationProcessor : DurationProcessor() {

    override fun getSql(): String {
        return ""
    }

    override fun getHandledEntityType(): String {
        return entityType
    }

    override fun getPropertyTypeForStart(): String {
        return start
    }

    override fun getPropertyTypeForEnd(): String {
        return end
    }

    override fun getPropertyTypeForDuration(): String {
        return duration
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.MINUTES
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.HOURS
    }
}

class MockEndDateProcessor : BaseDurationProcessor() {

    override fun getSql(): String {
        return ""
    }

    override fun getHandledEntityType(): String {
        return entityType
    }

    override fun getPropertyTypeForStart(): String {
        return start
    }

    override fun getPropertyTypeForEnd(): String {
        return end
    }

    override fun getPropertyTypeForDuration(): String {
        return duration
    }

    override fun getCalculationTimeUnit(): ChronoUnit {
        return ChronoUnit.MINUTES
    }

    override fun getDisplayTimeUnit(): ChronoUnit {
        return ChronoUnit.HOURS
    }

    override fun getInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(getHandledEntityType()) to
                setOf(FullQualifiedName(getPropertyTypeForStart()), FullQualifiedName(getPropertyTypeForDuration())))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(getHandledEntityType()), FullQualifiedName(getPropertyTypeForEnd()))
    }
}

class MockAssociationProcessor : AssociationProcessor {
    override fun getSrcInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf()
    }

    override fun getEdgeInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf()
    }

    override fun getDstInputs(): Map<FullQualifiedName, Set<FullQualifiedName>> {
        return mapOf(FullQualifiedName(assocType) to setOf(FullQualifiedName(assocInput)))
    }

    override fun getOutput(): Pair<FullQualifiedName, FullQualifiedName> {
        return Pair(FullQualifiedName(entityType), FullQualifiedName(end))
    }

    override fun getSql(): String {
        return ""
    }

    override fun getFilters(): Map<FullQualifiedName, Map<FullQualifiedName, ValueFilter<*>>> {
        return mapOf()
    }

}