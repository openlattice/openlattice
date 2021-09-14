

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

package com.openlattice.authorization.processors;

import static com.google.common.base.Preconditions.checkNotNull;

import com.hazelcast.core.Offloadable;
import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractMerger;
import com.openlattice.authorization.AceValue;

import java.time.OffsetDateTime;
import java.util.EnumSet;

public class PermissionMerger extends AbstractMerger<AceKey, AceValue, Permission> implements Offloadable {
    private static final long serialVersionUID = -3504613417625318717L;
    private final SecurableObjectType securableObjectType;
    private final OffsetDateTime expirationDate;

    public  PermissionMerger(
            Iterable<Permission> objects,
            SecurableObjectType securableObjectType) {
        super( objects );
        this.securableObjectType = checkNotNull( securableObjectType );
        this.expirationDate = OffsetDateTime.MAX;
    }

    public  PermissionMerger(
            Iterable<Permission> objects,
            SecurableObjectType securableObjectType,
            OffsetDateTime expirationDate) {
        super( objects );
        this.securableObjectType = checkNotNull( securableObjectType );
        this.expirationDate = expirationDate;
    }

    @Override protected void processBeforeWriteBack( AceValue value ) {
        value.setSecurableObjectType( securableObjectType );
        value.setExpirationDate( expirationDate );
    }

    @Override
    protected AceValue newEmptyCollection() {
        return new AceValue( EnumSet.noneOf( Permission.class ), securableObjectType, expirationDate );
    }

    public SecurableObjectType getSecurableObjectType() {
        return securableObjectType;
    }

    public OffsetDateTime getExpirationDate() {
        return expirationDate;
    }

    @Override public String getExecutorName() {
        return OFFLOADABLE_EXECUTOR;
    }
}
