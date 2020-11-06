package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.authorization.aggregators.PrincipalAggregator
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.PrincipalSet

class PrincipalAggregatorStreamSerializerTest
    : AbstractStreamSerializerTest<PrincipalAggregatorStreamSerializer, PrincipalAggregator>() {
    override fun createSerializer(): PrincipalAggregatorStreamSerializer {
        return PrincipalAggregatorStreamSerializer()
    }

    override fun createInput(): PrincipalAggregator {
        val principalsMap = mutableMapOf(
                TestDataFactory.aclKey() to
                        PrincipalSet(mutableSetOf(TestDataFactory.userPrincipal(), TestDataFactory.rolePrincipal())),
                TestDataFactory.aclKey() to
                        PrincipalSet(mutableSetOf()))
        val aggregator = PrincipalAggregator(principalsMap)

        return aggregator
    }
}