package com.openlattice.edm.events

import java.util.UUID

data class LinkedEntitySetRemovedEvent(
        val linkingEntitySetId: UUID,
        val remainingLinkingIdsByEntitySetId: Map<UUID, Set<UUID>>,
        val removedLinkingIds: Set<UUID>)