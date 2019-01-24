/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.hazelcast.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.serializers.AclKeyKryoSerializer;
import com.openlattice.authorization.serializers.EntityDataLambdasStreamSerializer;
import com.openlattice.conductor.rpc.*;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.organization.Organization;
import com.openlattice.organizations.PrincipalSet;
import com.openlattice.organizations.serializers.DelegatedStringSetKryoSerializer;
import com.openlattice.organizations.serializers.DelegatedUUIDSetKryoSerializer;
import com.openlattice.organizations.serializers.PrincipalSetKryoSerializer;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.openlattice.search.requests.SearchConstraints;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.invoke.SerializedLambda;
import java.util.UUID;
import java.util.function.Function;

@SuppressWarnings( "rawtypes" )
@Component
public class ConductorElasticsearchCallStreamSerializer
        implements SelfRegisteringStreamSerializer<ConductorElasticsearchCall> {
    private static final ThreadLocal<Kryo> kryoThreadLocal = new ThreadLocal<Kryo>() {

        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();

            // https://github.com/EsotericSoftware/kryo/blob/master/test/com/esotericsoftware/kryo/serializers/Java8ClosureSerializerTest.java
            kryo.setInstantiatorStrategy(
                    new Kryo.DefaultInstantiatorStrategy(
                            new StdInstantiatorStrategy() ) );
            kryo.register( Object[].class );
            kryo.register( java.lang.Class.class );

            kryo.register( Organization.class);
            kryo.register( DelegatedStringSet.class, new DelegatedStringSetKryoSerializer() );
            kryo.register( DelegatedUUIDSet.class, new DelegatedUUIDSetKryoSerializer() );
            kryo.register( PrincipalSet.class, new PrincipalSetKryoSerializer() );


            // Shared Lambdas
            kryo.register( ElasticsearchLambdas.class );
            kryo.register( EntityDataLambdas.class, new EntityDataLambdasStreamSerializer() );
            kryo.register( BulkEntityDataLambdas.class, new BulkEntityDataLambdasStreamSerializer() );
            kryo.register( BulkLinkedDataLambdas.class, new BulkLinkedDataLambdasStreamSerializer() );
            kryo.register( SearchConstraints.class, new SearchConstraintsStreamSerializer() );
            kryo.register( SearchWithConstraintsLambda.class, new SearchWithConstraintsLambdaStreamSerializer() );
            kryo.register( SerializedLambda.class );
            kryo.register( AclKey.class, new AclKeyKryoSerializer() );

            // always needed for closure serialization, also if
            // registrationRequired=false
            kryo.register( ClosureSerializer.Closure.class,
                    new ClosureSerializer() );
            kryo.register( Function.class,
                    new ClosureSerializer() );

            kryo.register( AclKey.class, new AclKeyKryoSerializer() );

            return kryo;
        }
    };

    private ConductorElasticsearchApi api;

    @Override
    @SuppressFBWarnings
    public void write( ObjectDataOutput out, ConductorElasticsearchCall object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getUserId() );
        Jdk8StreamSerializers.serializeWithKryo( kryoThreadLocal.get(), out, object.getFunction(), 32 );
    }

    @Override
    @SuppressWarnings( "unchecked" )
    @SuppressFBWarnings
    public ConductorElasticsearchCall read( ObjectDataInput in ) throws IOException {
        UUID userId = UUIDStreamSerializer.deserialize( in );
        Function<ConductorElasticsearchApi, ?> f = (Function<ConductorElasticsearchApi, ?>) Jdk8StreamSerializers
                .deserializeWithKryo( kryoThreadLocal.get(), in, 32 );
        return new ConductorElasticsearchCall( userId, f, api );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.CONDUCTOR_ELASTICSEARCH_CALL.ordinal();
    }

    @Override
    public void destroy() {

    }

    public synchronized void setConductorElasticsearchApi( ConductorElasticsearchApi api ) {
        Preconditions.checkState( this.api == null, "Api can only be set once" );
        this.api = Preconditions.checkNotNull( api );
    }

    @Override
    public Class<? extends ConductorElasticsearchCall> getClazz() {
        return ConductorElasticsearchCall.class;
    }

}
