

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

package com.openlattice.data;

import java.util.UUID;

public class TicketKey {
    private final String principalId;
    private final UUID   ticket;

    public TicketKey( String principalId, UUID ticket ) {
        this.principalId = principalId;
        this.ticket = ticket;
    }

    public TicketKey( String principalId ) {
        this( principalId, UUID.randomUUID() );
    }

    public String getPrincipalId() {
        return principalId;
    }

    public UUID getTicket() {
        return ticket;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( principalId == null ) ? 0 : principalId.hashCode() );
        result = prime * result + ( ( ticket == null ) ? 0 : ticket.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( !( obj instanceof TicketKey ) ) {
            return false;
        }
        TicketKey other = (TicketKey) obj;
        if ( principalId == null ) {
            if ( other.principalId != null ) {
                return false;
            }
        } else if ( !principalId.equals( other.principalId ) ) {
            return false;
        }
        if ( ticket == null ) {
            if ( other.ticket != null ) {
                return false;
            }
        } else if ( !ticket.equals( other.ticket ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "TicketKey [principalId=" + principalId + ", ticket=" + ticket + "]";
    }

}
