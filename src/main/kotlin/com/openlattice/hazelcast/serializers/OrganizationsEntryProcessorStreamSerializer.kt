package com.openlattice.hazelcast.serializers

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.ClosureSerializer
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.serializers.AclKeyKryoSerializer
import com.openlattice.authorization.serializers.EntityDataLambdasStreamSerializer
import com.openlattice.conductor.rpc.*
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organizations.Organization
import com.openlattice.organizations.PrincipalSet
import com.openlattice.organizations.processors.OrganizationEntryProcessor
import com.openlattice.organizations.serializers.DelegatedStringSetKryoSerializer
import com.openlattice.organizations.serializers.DelegatedUUIDSetKryoSerializer
import com.openlattice.organizations.serializers.PrincipalSetKryoSerializer
import com.openlattice.rhizome.hazelcast.DelegatedStringSet
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet
import com.openlattice.search.requests.SearchConstraints
import org.objenesis.strategy.StdInstantiatorStrategy
import org.springframework.stereotype.Component
import java.lang.invoke.SerializedLambda
import java.util.function.Function

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class OrganizationsEntryProcessorStreamSerializer :SelfRegisteringStreamSerializer<OrganizationEntryProcessor> {
    private val kryoThreadLocal = object : ThreadLocal<Kryo>() {

        override fun initialValue(): Kryo {
            val kryo = Kryo()

            // https://github.com/EsotericSoftware/kryo/blob/master/test/com/esotericsoftware/kryo/serializers/Java8ClosureSerializerTest.java
            kryo.instantiatorStrategy = Kryo.DefaultInstantiatorStrategy(
                    StdInstantiatorStrategy()
            )
            kryo.register(Array<Any>::class.java)
            kryo.register(java.lang.Class::class.java)

            kryo.register(Organization::class.java)
            kryo.register(DelegatedStringSet::class.java, DelegatedStringSetKryoSerializer())
            kryo.register(DelegatedUUIDSet::class.java, DelegatedUUIDSetKryoSerializer())
            kryo.register(PrincipalSet::class.java, PrincipalSetKryoSerializer())

            //Shared Lambdas
            kryo.register(SerializedLambda::class.java)
            kryo.register(AclKey::class.java, AclKeyKryoSerializer())

            // always needed for closure serialization, also if
            // registrationRequired=false
            kryo.register(
                    ClosureSerializer.Closure::class.java,
                    ClosureSerializer()
            )
            kryo.register(OrganizationEntryProcessor::class.java,ClosureSerializer())

            kryo.register(AclKey::class.java, AclKeyKryoSerializer())

            return kryo
        }
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ORGANIZATION_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out OrganizationEntryProcessor> {
        return OrganizationEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: OrganizationEntryProcessor) {
        Jdk8StreamSerializers.serializeWithKryo(kryoThreadLocal.get(), out, obj, 32)
    }

    override fun read(input: ObjectDataInput): OrganizationEntryProcessor {
        return Jdk8StreamSerializers.deserializeWithKryo(kryoThreadLocal.get(), input, 32) as OrganizationEntryProcessor
    }
}