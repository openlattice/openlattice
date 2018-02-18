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

package com.openlattice.mail;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class EmailRequest {
    protected static final String    FROM_FIELD = "from";
    protected static final String    TO_FIELD   = "to";
    protected static final String    CC_FIELD   = "cc";
    protected static final String    BCC_FIELD  = "bcc";
    private final Optional<String>   from;
    private final String[]           to;
    private final Optional<String[]> cc;
    private final Optional<String[]> bcc;

    public EmailRequest(
            Optional<String> from,
            String[] to,
            Optional<String[]> cc,
            Optional<String[]> bcc ) {

        this.from = Preconditions.checkNotNull( from );
        this.to = ImmutableList.copyOf( Iterables.filter( Arrays.asList( Preconditions.checkNotNull( to ) ),
                new Predicate<String>() {

                    @Override
                    public boolean apply( String input ) {
                        return StringUtils.isNotBlank( input );
                    }

                } ) ).toArray( new String[ 0 ] );
        this.cc = Preconditions.checkNotNull( cc );
        this.bcc = Preconditions.checkNotNull( bcc );
        Preconditions.checkState( this.to.length > 0 );
    }

    @JsonProperty( FROM_FIELD )
    public Optional<String> getFrom() {
        return from;
    }

    @JsonProperty( TO_FIELD )
    public String[] getTo() {
        return to;
    }

    @JsonProperty( CC_FIELD )
    public Optional<String[]> getCc() {
        return cc;
    }

    @JsonProperty( BCC_FIELD )
    public Optional<String[]> getBcc() {
        return bcc;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( bcc == null ) ? 0 : bcc.hashCode() );
        result = prime * result + ( ( cc == null ) ? 0 : cc.hashCode() );
        result = prime * result + ( ( from == null ) ? 0 : from.hashCode() );
        result = prime * result + Arrays.hashCode( to );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        EmailRequest other = (EmailRequest) obj;
        if ( bcc == null ) {
            if ( other.bcc != null ) return false;
        } else if ( !bcc.equals( other.bcc ) ) return false;
        if ( cc == null ) {
            if ( other.cc != null ) return false;
        } else if ( !cc.equals( other.cc ) ) return false;
        if ( from == null ) {
            if ( other.from != null ) return false;
        } else if ( !from.equals( other.from ) ) return false;
        if ( !Arrays.equals( to, other.to ) ) return false;
        return true;
    }

}