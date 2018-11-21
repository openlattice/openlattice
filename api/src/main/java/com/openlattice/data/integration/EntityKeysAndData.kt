package com.openlattice.data.integration

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.*

/* Contains entity set id mapped entity id, and entity key id of a given entity
* and a map of property type id to properties that belong to that entity
*/

data class EntityKeysAndData(
        @JsonProperty val entitySetIdToIdsAndKeys: MutableMap<UUID, Map<String, UUID>>,
        @JsonProperty val properties: MutableMap<UUID, Set<Any>>)