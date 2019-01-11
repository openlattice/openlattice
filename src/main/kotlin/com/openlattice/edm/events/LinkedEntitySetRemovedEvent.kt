package com.openlattice.edm.events

import java.util.UUID

data class LinkedEntitySetRemovedEvent(
        val linkingEntitySetId: UUID,
        val remainingLinkedEntitySets: Set<UUID>,
        val removedLinkedEntitySets: Set<UUID>)