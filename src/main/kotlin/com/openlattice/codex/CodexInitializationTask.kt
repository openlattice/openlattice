package com.openlattice.codex

import com.openlattice.collections.CollectionTemplateType
import com.openlattice.collections.EntityTypeCollection
import com.openlattice.edm.tasks.EdmSyncInitializerTask
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*
import kotlin.collections.LinkedHashSet

val TEMPLATE_TYPES = LinkedHashSet(listOf(
        CollectionTemplateType(Optional.empty(), "app.contactinfogiven", "Contact Info Given", Optional.empty(), UUID.fromString("cf9b4d36-6f1a-4e6f-94f0-6458054fc567")),
        CollectionTemplateType(Optional.empty(), "app.sentfrom", "Sent From", Optional.empty(), UUID.fromString("cf9b4d36-6f1a-4e6f-94f0-6458054fc567")),
        CollectionTemplateType(Optional.empty(), "app.sentto", "Sent To", Optional.empty(), UUID.fromString("766e8284-fb5d-4d38-9595-99b6bca99a3b")),
        CollectionTemplateType(Optional.empty(), "app.contactinformation", "Contact Information", Optional.empty(), UUID.fromString("2e20c21e-6448-4a2d-bc57-9a5f2c45b589")),
        CollectionTemplateType(Optional.empty(), "app.settings", "Settings", Optional.empty(), UUID.fromString("bc0f0785-6af4-4a16-ab01-ef125d8fa183")),
        CollectionTemplateType(Optional.empty(), "app.subjectof", "Subject Of", Optional.empty(), UUID.fromString("34aeaca4-d424-43cd-a3b4-f2032d583280")),
        CollectionTemplateType(Optional.empty(), "app.people", "People", Optional.empty(), UUID.fromString("31cf5595-3fe9-4d3e-a9cf-39355a4b8cab")),
        CollectionTemplateType(Optional.empty(), "app.messages", "Messages", Optional.empty(), UUID.fromString("8d0e38de-4302-4ec2-a1a7-f9159086e60e")),
        CollectionTemplateType(Optional.empty(), "app.clients", "Clients", Optional.empty(), UUID.fromString("31cf5595-3fe9-4d3e-a9cf-39355a4b8cab"))
))

private val ENTITY_TYPE_COLLECTION_NAME = FullQualifiedName("app.codex")

class CodexInitializationTask : HazelcastInitializationTask<CodexInitializationTaskDependencies> {
    override fun getInitialDelay(): Long {
        return 0
    }

    override fun initialize(dependencies: CodexInitializationTaskDependencies) {

        if (dependencies.reservations.isReserved(ENTITY_TYPE_COLLECTION_NAME.toString())) {
            return
        }

        dependencies.collectionManager.createEntityTypeCollection(EntityTypeCollection(
                Optional.empty(),
                ENTITY_TYPE_COLLECTION_NAME,
                "Codex",
                Optional.empty(),
                setOf(),
                TEMPLATE_TYPES
        ))
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(EdmSyncInitializerTask::class.java)
    }

    override fun getName(): String {
        return Task.CODEX_INITIALIZER.name
    }

    override fun getDependenciesClass(): Class<out CodexInitializationTaskDependencies> {
        return CodexInitializationTaskDependencies::class.java
    }

}