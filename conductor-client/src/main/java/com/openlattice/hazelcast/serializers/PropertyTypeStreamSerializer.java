

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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.edm.type.Analyzer;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.postgres.IndexType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Component
public class PropertyTypeStreamSerializer implements SelfRegisteringStreamSerializer<PropertyType> {

    @Override
    public void write( ObjectDataOutput out, PropertyType object ) throws IOException {
        serialize( out, object );
    }

    @Override
    public PropertyType read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.PROPERTY_TYPE.ordinal();
    }

    public static void serialize( ObjectDataOutput out, PropertyType object ) throws IOException {
        UUIDStreamSerializerUtils.serialize( out, object.getId() );
        FullQualifiedNameStreamSerializer.serialize( out, object.getType() );
        out.writeUTF( object.getTitle() );
        out.writeUTF( object.getDescription() );
        SetStreamSerializers.serialize( out,
                object.getSchemas(),
                ( FullQualifiedName schema ) -> FullQualifiedNameStreamSerializer.serialize( out, schema ) );
        EdmPrimitiveTypeKindStreamSerializer.serialize( out, object.getDatatype() );
        out.writeBoolean( object.isPii() );
        AnalyzerStreamSerializer.serialize( out, object.getAnalyzer() );
        IndexTypeStreamSerializer.serialize( out, object.getPostgresIndexType() );
        SetStreamSerializers.fastStringSetSerialize( out, object.getEnumValues() );
    }

    public static PropertyType deserialize( ObjectDataInput in ) throws IOException {
        UUID id = UUIDStreamSerializerUtils.deserialize( in );
        FullQualifiedName type = FullQualifiedNameStreamSerializer.deserialize( in );
        String title = in.readUTF();
        Optional<String> description = Optional.of( in.readUTF() );
        Set<FullQualifiedName> schemas = SetStreamSerializers.deserialize( in, FullQualifiedNameStreamSerializer::deserialize );
        EdmPrimitiveTypeKind datatype = EdmPrimitiveTypeKindStreamSerializer.deserialize( in );
        Optional<Boolean> piiField = Optional.of( in.readBoolean() );
        Optional<Analyzer> analyzer = Optional.of( AnalyzerStreamSerializer.deserialize( in ) );
        Optional<IndexType> indexMethod = Optional.of( IndexTypeStreamSerializer.deserialize( in ) );
        Optional<Set<String>> enumValues = Optional.of( SetStreamSerializers.orderedFastStringSetDeserialize( in ));
        return new PropertyType( id, type, title, description, schemas, datatype, enumValues, piiField, analyzer, indexMethod );
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<PropertyType> getClazz() {
        return PropertyType.class;
    }

}
