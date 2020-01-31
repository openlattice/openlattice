package com.openlattice.apps.historical

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.Principal
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.organizations.Organization
import java.util.*

class HistoricalAppConfig(
        @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
        @JsonProperty(SerializationConstants.PRINCIPAL) principal: Principal,
        @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>,
        @JsonProperty(SerializationConstants.APP_ID) val appId: UUID,
        @JsonProperty(SerializationConstants.ORGANIZATION) val organization: Organization,
        @JsonProperty(SerializationConstants.CONFIG) val config: Map<String, HistoricalAppTypeSetting>
) : SecurablePrincipal(id, principal, title, description)