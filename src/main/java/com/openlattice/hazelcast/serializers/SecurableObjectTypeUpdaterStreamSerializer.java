/*
 * Copyright (C) 2017. OpenLattice, Inc
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.processors.SecurableObjectTypeUpdater;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import java.io.IOException;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class SecurableObjectTypeUpdaterStreamSerializer
        implements SelfRegisteringStreamSerializer<SecurableObjectTypeUpdater> {
    @Override public Class<SecurableObjectTypeUpdater> getClazz() {
        return SecurableObjectTypeUpdater.class;
    }

    @Override public void write( ObjectDataOutput out, SecurableObjectTypeUpdater object ) throws IOException {
        AceValueStreamSerializer.serialize( out, object.getSecurableObjectType() );
    }

    @Override public SecurableObjectTypeUpdater read( ObjectDataInput in ) throws IOException {
        return new SecurableObjectTypeUpdater( AceValueStreamSerializer.deserialize( in ) );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.SECURABLE_OBJECT_TYPE_UPDATE.ordinal();
    }

    @Override public void destroy() {

    }
}
