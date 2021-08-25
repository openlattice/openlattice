package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.postgres.PostgresDatatype
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Component
class PostgresDatatypeStreamSerializer: AbstractEnumSerializer<PostgresDatatype>() {

    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: PostgresDatatype) =  AbstractEnumSerializer.serialize(out, `object`)
        @JvmStatic
        fun deserialize(`in`: ObjectDataInput): PostgresDatatype = deserialize(PostgresDatatype::class.java, `in`)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.POSTGRES_DATATYPE.ordinal
    }

    override fun getClazz(): Class<PostgresDatatype> {
        return PostgresDatatype::class.java
    }
}