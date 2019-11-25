package com.openlattice.apps

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class AppConfigKey(
        @JsonProperty(SerializationConstants.APP_ID) val appId: UUID,
        @JsonProperty( SerializationConstants.ORGANIZATION_ID )  val organizationId: UUID,
        @JsonProperty( SerializationConstants.APP_TYPE_ID ) val appTypeId: UUID
)