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

package com.openlattice.authorization.processors;

import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import com.openlattice.authorization.AceValue;
import java.util.Map.Entry;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class SecurableObjectTypeUpdater extends AbstractRhizomeEntryProcessor<AceKey,AceValue,Void> {
    private final SecurableObjectType securableObjectType;

    public SecurableObjectTypeUpdater( SecurableObjectType securableObjectType ) {
        this.securableObjectType = securableObjectType;
    }

    @Override public Void process( Entry<AceKey,AceValue> entry ) {
        AceValue value = entry.getValue();
        if(value!=null) {
            value.setSecurableObjectType( securableObjectType );
            entry.setValue( value );
        }
        return null;
    }

    public SecurableObjectType getSecurableObjectType() {
        return securableObjectType;
    }
}
