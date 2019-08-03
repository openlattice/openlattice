package com.openlattice.collections


import com.google.common.collect.Maps
import java.util.UUID

data class CollectionTemplates(
        val templates: MutableMap<UUID, MutableMap<UUID, UUID>> = Maps.newHashMap()
) {

    @Synchronized
    fun put(entitySetCollectionId: UUID, templateTypeId: UUID, entitySetId: UUID) {
        val template = templates.getOrDefault(entitySetCollectionId, Maps.newHashMap())
        template[templateTypeId] = entitySetId
        templates[entitySetCollectionId] = template
    }

    @Synchronized
    fun putAll(templateValues: Map<UUID, MutableMap<UUID, UUID>>) {
        for ((key, value) in templateValues) {
            val template = templates.getOrDefault(key, Maps.newHashMap())
            template.putAll(value)
            templates[key] = template
        }
    }
}