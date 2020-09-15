package com.openlattice.collections


import com.google.common.collect.Maps
import java.util.*

/**
 * A Hazelcast-serializable wrapper around a MutableMap<UUID, MutableMap<UUID, UUID>>
 * for processing entity set collection templates.
 *
 * @param templates A map from entity set collection ids to their templates,
 * where their templates are maps from collection template type ids to entity set ids
 */
data class CollectionTemplates(
        val templates: MutableMap<UUID, MutableMap<UUID, UUID>> = mutableMapOf()
) {

    fun put(entitySetCollectionId: UUID, templateTypeId: UUID, entitySetId: UUID) {
        val template = templates.getOrDefault(entitySetCollectionId, Maps.newConcurrentMap())
        template[templateTypeId] = entitySetId
        templates[entitySetCollectionId] = template
    }

    fun putAll(templateValues: Map<UUID, MutableMap<UUID, UUID>>) {
        templateValues.forEach { (key, value) ->
            val template = templates.getOrDefault(key, Maps.newConcurrentMap())
            template.putAll(value)
            templates[key] = template
        }
    }
}