package com.openlattice.collections

import java.util.UUID

data class CollectionTemplateKey(
        val entitySetCollectionId: UUID,
        val templateTypeId: UUID)