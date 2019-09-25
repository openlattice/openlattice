package com.openlattice.collections

import java.util.UUID

/**
 * A collection template key can be used to point to an entity set id
 * by looking up the collection template type id in the entity set collection's template map
 *
 * @param entitySetCollectionId The id of the entity set collection.
 * @param templateTypeId The id of the collection template type.
 */
data class CollectionTemplateKey(
        val entitySetCollectionId: UUID,
        val templateTypeId: UUID)