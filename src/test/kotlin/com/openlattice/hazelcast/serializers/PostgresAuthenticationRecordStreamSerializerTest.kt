package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresAuthenticationRecord

class PostgresAuthenticationRecordStreamSerializerTest : AbstractStreamSerializerTest<PostgresAuthenticationRecordStreamSerializer, PostgresAuthenticationRecord>() {

        override fun createSerializer(): PostgresAuthenticationRecordStreamSerializer {
            return PostgresAuthenticationRecordStreamSerializer()
        }

        override fun createInput(): PostgresAuthenticationRecord {
            return TestDataFactory.postgresAuthenticationRecord()
        }
}