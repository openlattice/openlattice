package com.openlattice.notifications.sms

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class SmsEntitySetInformation(
        @JsonProperty(SerializationConstants.PHONE_NUMBER) val phoneNumber: String,
        @JsonProperty(SerializationConstants.ORGANIZATION_ID) val organizationId: UUID,
        @JsonProperty(SerializationConstants.ENTITY_SET_ID) val entitySetIds: Set<UUID>,
        @JsonProperty(SerializationConstants.TAGS) val tags: Set<String>
)