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
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingBiConsumer;
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingConsumer;
import com.kryptnostic.rhizome.hazelcast.serializers.IoPerformingFunction;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Factory method for stream serializing Guava Optionals in hazelcast.
 *
 * @author Ho Chung Siu
 */
public final class OptionalStreamSerializers {
    private OptionalStreamSerializers() {
    }

    // Serialize single optional
    public static <T> void serialize( ObjectDataOutput out, Optional<T> element, IoPerformingConsumer<T> c )
            throws IOException {
        final boolean present = element.isPresent();
        out.writeBoolean( present );
        if ( present ) {
            c.accept( element.get() );
        }
    }

    public static <T> void serialize(
            ObjectDataOutput out,
            Optional<T> element,
            IoPerformingBiConsumer<ObjectDataOutput, T> c ) throws IOException {
        final boolean present = element.isPresent();
        out.writeBoolean( present );
        if ( present ) {
            c.accept( out, element.get() );
        }
    }

    public static <T> Optional<T> deserialize( ObjectDataInput in, IoPerformingFunction<ObjectDataInput, T> f )
            throws IOException {
        if ( in.readBoolean() ) {
            T elem = f.apply( in );
            return ( elem == null ) ? Optional.empty() : Optional.of( elem );
        } else {
            return Optional.empty();
        }
    }

    // Serialize set of optional
    public static <T> void serializeSet( ObjectDataOutput out, Optional<Set<T>> elements, IoPerformingConsumer<T> c )
            throws IOException {
        final boolean present = elements.isPresent();
        out.writeBoolean( present );
        if ( present ) {
            SetStreamSerializers.serialize( out, elements.get(), c );
        }
    }

    public static <T> void serializeSet(
            ObjectDataOutput out,
            Optional<Set<T>> elements,
            IoPerformingBiConsumer<ObjectDataOutput, T> c ) throws IOException {
        final boolean present = elements.isPresent();
        out.writeBoolean( present );
        if ( present ) {
            SetStreamSerializers.serialize( out, elements.get(), c );
        }
    }

    public static <T> Optional<Set<T>> deserializeSet( ObjectDataInput in, IoPerformingFunction<ObjectDataInput, T> f )
            throws IOException {
        if ( in.readBoolean() ) {
            Set<T> elements = SetStreamSerializers.deserialize( in, f );
            return Optional.of( elements );
        } else {
            return Optional.empty();
        }
    }

    public static <T> Optional<LinkedHashSet<T>> deserializeLinkedHashSet( ObjectDataInput in, IoPerformingFunction<ObjectDataInput, T> f )
            throws IOException {
        if ( in.readBoolean() ) {
            LinkedHashSet<T> elements = SetStreamSerializers.orderedDeserialize(  in, f );
            return Optional.of( elements );
        } else {
            return Optional.empty();
        }
    }
}
