/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.client.serialization;

import com.openlattice.client.ApiClient;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.edm.EdmApi;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ApiClientKryoTest {
    private static Kryo kryo = new Kryo();

    static {
        kryo.setInstantiatorStrategy(
                new Kryo.DefaultInstantiatorStrategy(
                        new StdInstantiatorStrategy() ) );
        kryo.register( Object[].class );
        kryo.register( java.lang.Class.class );
        kryo.register( ClosureSerializer.Closure.class,
                new ClosureSerializer() );
    }

    @Test
    public void testSerdes() {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Output output = new Output( os );
        ApiClient expected = new ApiClient( RetrofitFactory.Environment.TESTING, () -> "sometoken" );
        kryo.writeClassAndObject( output, expected );
        output.flush();

        byte[] bytes = os.toByteArray();
        ByteArrayInputStream is = new ByteArrayInputStream( bytes );
        Input input = new Input( is );
        ApiClient actual = (ApiClient) kryo.readClassAndObject( input );

        //This will null pointer if custom initialization isn't handled correctly.
        Assert.assertNotNull( actual.get().create( EdmApi.class ) );
    }
}
