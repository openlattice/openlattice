

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

package com.openlattice.conductor.rpc;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;

@Component
public class LambdaStreamSerializer implements TestableSelfRegisteringStreamSerializer<Runnable> {
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial( () -> {
        Kryo kryo = new Kryo();
        // Stuff from
        // https://github.com/EsotericSoftware/kryo/blob/master/test/com/esotericsoftware/kryo/serializers/Java8ClosureSerializerTest.java
        kryo.setInstantiatorStrategy( new Kryo.DefaultInstantiatorStrategy( new StdInstantiatorStrategy() ) );
        kryo.register( Object[].class );
        kryo.register( Class.class );

        // Shared Lambdas
        kryo.register( SerializedLambda.class );

        // always needed for closure serialization, also if registrationRequired=false
        kryo.register( ClosureSerializer.Closure.class, new ClosureSerializer() );

        kryo.register( Runnable.class, new ClosureSerializer() );

        return kryo;
    } );

    @Override
    @SuppressFBWarnings
    public void write( ObjectDataOutput out, Runnable object ) throws IOException {
        Output output = new Output( (OutputStream) out );
        kryoThreadLocal.get().writeClassAndObject( output, object );
        output.flush();
    }

    @Override
    @SuppressFBWarnings
    public Runnable read( ObjectDataInput in ) throws IOException {
        Input input = new Input( (InputStream) in );
        return (Runnable) kryoThreadLocal.get().readClassAndObject( input );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.RUNNABLE.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<Runnable> getClazz() {
        return Runnable.class;
    }

    @Override public Runnable generateTestValue() {
        return (Runnable & Serializable) () -> System.out.println( "foo" );
    }
}
