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

package com.openlattice.data;

import com.google.common.collect.SetMultimap;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataAssociation {
    private final UUID srcEntitySetId;
    private final int  srcEntityIndex;

    private final UUID dstEntitySetId;
    private final int  dstEntityIndex;

    private final SetMultimap<UUID, Object> data;

    public DataAssociation(
            UUID srcEntitySetId,
            int srcEntityIndex,
            UUID dstEntitySetId,
            int dstEntityIndex,
            SetMultimap<UUID, Object> data ) {
        this.srcEntitySetId = srcEntitySetId;
        this.srcEntityIndex = srcEntityIndex;
        this.dstEntitySetId = dstEntitySetId;
        this.dstEntityIndex = dstEntityIndex;
        this.data = data;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof DataAssociation ) ) { return false; }
        DataAssociation that = (DataAssociation) o;
        return srcEntityIndex == that.srcEntityIndex &&
                dstEntityIndex == that.dstEntityIndex &&
                Objects.equals( srcEntitySetId, that.srcEntitySetId ) &&
                Objects.equals( dstEntitySetId, that.dstEntitySetId ) &&
                Objects.equals( data, that.data );
    }

    @Override public int hashCode() {

        return Objects.hash( srcEntitySetId, srcEntityIndex, dstEntitySetId, dstEntityIndex, data );
    }

    @Override public String toString() {
        return "DataAssociation{" +
                "srcEntitySetId=" + srcEntitySetId +
                ", srcEntityIndex=" + srcEntityIndex +
                ", dstEntitySetId=" + dstEntitySetId +
                ", dstEntityIndex=" + dstEntityIndex +
                ", data=" + data +
                '}';
    }

    public UUID getSrcEntitySetId() {
        return srcEntitySetId;
    }

    public int getSrcEntityIndex() {
        return srcEntityIndex;
    }

    public UUID getDstEntitySetId() {
        return dstEntitySetId;
    }

    public int getDstEntityIndex() {
        return dstEntityIndex;
    }

    public SetMultimap<UUID, Object> getData() {
        return data;
    }
}
