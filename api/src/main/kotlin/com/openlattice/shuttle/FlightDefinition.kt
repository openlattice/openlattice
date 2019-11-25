package com.openlattice.shuttle

import com.fasterxml.jackson.annotation.JsonIgnore
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import java.util.*

class FlightDefinition

constructor(
        id: Optional<UUID>,
        title: String,
        description: Optional<String>,
        sql: String,
        dataSource: String
): AbstractSecurableObject(id, title, description) {

    constructor(
            id: UUID,
            title: String,
            description: Optional<String>,
            sql: String,
            dataSource: String
    ): this(Optional.of<UUID>(id), title, description, sql, dataSource)

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}
