

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

import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import java.io.IOException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

@Component
public class FullQualifiedNameStreamSerializer implements SelfRegisteringStreamSerializer<FullQualifiedName> {

	@Override
	public void write(ObjectDataOutput out, FullQualifiedName object)
			throws IOException {
		serialize( out, object );
	}

	@Override
	public FullQualifiedName read(ObjectDataInput in) throws IOException {
		return deserialize( in );
	}

    @Override
	public int getTypeId() {
		return StreamSerializerTypeIds.FULL_QUALIFIED_NAME.ordinal();
	}

	@Override
	public void destroy() {
		
	}

	@Override
	public Class<FullQualifiedName> getClazz() {
		return FullQualifiedName.class;
	}
	
	public static void serialize( ObjectDataOutput out, FullQualifiedName object ) throws IOException {
	    out.writeUTF( object.getNamespace() );
        out.writeUTF( object.getName() );
	}
	
	public static  FullQualifiedName  deserialize( ObjectDataInput in ) throws IOException {
        String namespace = in.readUTF();
        String name = in.readUTF();
        return new FullQualifiedName( namespace, name );
        
    }
}
