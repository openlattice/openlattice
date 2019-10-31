package com.openlattice.postgres

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants

/**
 * Holds information for authentication records that will populate a pg_hba.conf file.
 *
 * @param type The connection type of the record.
 * @param database The name of the database that the record corresponds to
 * @param user The username that the record authenticates
 * @param ipAddress The IP address from which the user may connect
 * @param ipMask The mask for the IP address
 * @param method The authentication method
 */

data class PostgresAuthenticationRecord(
        @JsonProperty(SerializationConstants.CONNECTION_TYPE) val connectionType: String,
        @JsonProperty(SerializationConstants.DATABASE) val database: String,
        @JsonProperty(SerializationConstants.USER_ID) val userId: String,
        @JsonProperty(SerializationConstants.IP_ADDRESS) val ipAddress: String,
        @JsonProperty(SerializationConstants.IP_MASK) val ipMask: String,
        @JsonProperty(SerializationConstants.AUTHENTICATION_METHOD) val authenticationMethod: String
) {
}