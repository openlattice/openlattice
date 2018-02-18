

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

package com.openlattice.conductor.codecs;

import java.util.EnumSet;

import com.openlattice.authorization.Permission;
import com.openlattice.requests.RequestStatus;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.TypeCodec.AbstractCollectionCodec;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

public class EnumSetTypeCodec<T extends Enum<T>> extends AbstractCollectionCodec<T, EnumSet<T>> {

    // Specify cqlType and codec for Permission
    private Class<T> javaType;

    public EnumSetTypeCodec( TypeCodec<T> eltCodec ) {
        super( DataType.set( eltCodec.getCqlType() ), getTypeToken( eltCodec.getJavaType() ), eltCodec );
        javaType = (Class<T>) eltCodec.getJavaType().getRawType();
    }

    private static <T extends Enum<T>> TypeToken<EnumSet<T>> getTypeToken( TypeToken<T> eltType ) {
        return new TypeToken<EnumSet<T>>() {
            private static final long serialVersionUID = -6972369825357521908L;
        }.where( new TypeParameter<T>() {}, eltType );
    }

    @Override
    protected EnumSet<T> newInstance( int size ) {
        return EnumSet.noneOf( javaType );
    }

    // Type Token for Permission and RequestStatus, the main use case
    private static final TypeToken<EnumSet<Permission>>    enumSetPermission = new TypeToken<EnumSet<Permission>>() {};
    private static final TypeToken<EnumSet<RequestStatus>> enumSetStatus     = new TypeToken<EnumSet<RequestStatus>>() {};

    public static TypeToken<EnumSet<Permission>> getTypeTokenForEnumSetPermission() {
        return enumSetPermission;
    }

    public static TypeToken<EnumSet<RequestStatus>> getTypeTokenForEnumSetRequestStatus() {
        return enumSetStatus;
    }

}
