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
 *
 */

package com.openlattice.edm;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

public class EdmDiff {
    private final EntityDataModel present; //Properties present in old EDM but missing in new EDM
    private final EntityDataModel missing; //Properties missing in old EDM but present in new EDM
    private final EntityDataModel conflicts; //Properties conflicting with current EDM.

    public EdmDiff(
            @JsonProperty( SerializationConstants.PRESENT ) EntityDataModel present,
            @JsonProperty( SerializationConstants.MISSING ) EntityDataModel missing,
            @JsonProperty( SerializationConstants.CONFLICTS ) EntityDataModel conflicts ) {
        this.present = present;
        this.missing = missing;
        this.conflicts = conflicts;
    }

    @JsonProperty( SerializationConstants.PRESENT )
    public EntityDataModel getPresent() {
        return present;
    }

    @JsonProperty( SerializationConstants.MISSING )
    public EntityDataModel getMissing() {
        return missing;
    }

    @JsonProperty( SerializationConstants.CONFLICTS )
    public EntityDataModel getConflicts() {
        return conflicts;
    }

    public boolean getAreVersionsMatching() {
        return present.getVersion().equals( missing.getVersion() );

    }
}
