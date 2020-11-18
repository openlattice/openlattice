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

package com.openlattice.data.analytics;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class IncrementableWeightId implements Comparable<IncrementableWeightId>, Serializable {
    private static final long serialVersionUID = 418959970258424966L;

    private final UUID id;
    private       long weight;

    public IncrementableWeightId( UUID id, long weight ) {
        this.id = id;
        this.weight = weight;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof IncrementableWeightId ) ) { return false; }

        IncrementableWeightId that = (IncrementableWeightId) o;

        if ( weight != that.weight ) { return false; }
        return id.equals( that.id );
    }

    public void increment() {
        ++weight;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public int compareTo( IncrementableWeightId o ) {
        int value = Long.compare( weight, o.weight );
        if ( value == 0 ) {
            value = id.compareTo( o.id );
        }
        return value;
    }

    public long getWeight() {
        return weight;
    }

    public UUID getId() {
        return id;
    }

    public static IncrementableWeightId merge( IncrementableWeightId a, IncrementableWeightId b ) {
        a.weight += b.weight;
        return a;
    }
}
