package com.openlattice.postgres

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants

/**
 * Holds information for authentication records that will populate a pg_hba.conf file.
 *
 * @param connectionType The connection type of the record.
 * @param database The name of the database that the record corresponds to
 * @param username The username that the record authenticates
 * @param ipAddress The IP address from which the user may connect
 * @param authenticationMethod The authentication method
 */

data class PostgresAuthenticationRecord(
        @JsonProperty(SerializationConstants.CONNECTION_TYPE) val connectionType: PostgresConnectionType,
        @JsonProperty(SerializationConstants.DATABASE) val database: String,
        @JsonProperty(SerializationConstants.USERNAME) val username: String,
        @JsonProperty(SerializationConstants.IP_ADDRESSES) val ipAddress: String,
        @JsonProperty(SerializationConstants.AUTHENTICATION_METHOD) val authenticationMethod: String
) {
    fun buildHBAConfRecord(): String {
            return "${this.connectionType.toString().toLowerCase()}  " +
                    "${this.database}  " +
                    "${this.username.removeSurrounding("\"")}  " +
                    "${this.ipAddress}  " +
                    "${this.authenticationMethod}\n"
    }

    fun buildPostgresRecord(): String {
        return  "'${this.username}', " +
                "'${this.database}', " +
                "'${this.connectionType}', " +
                "'${this.ipAddress}', " +
                "'${this.authenticationMethod}'"
    }
}