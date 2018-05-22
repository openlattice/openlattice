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

package com.openlattice.linking.util;

import com.google.common.collect.Sets;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
public class PersonProperties {
    private static final Logger        logger = LoggerFactory.getLogger( PersonProperties.class );
    private static       DecimalFormat dd     = new DecimalFormat( "00" );

    private static FullQualifiedName FIRST_NAME_FQN     = new FullQualifiedName( "nc.PersonGivenName" );
    private static FullQualifiedName MIDDLE_NAME_FQN    = new FullQualifiedName( "nc.PersonMiddleName" );
    private static FullQualifiedName LAST_NAME_FQN      = new FullQualifiedName( "nc.PersonSurName" );
    private static FullQualifiedName SEX_FQN            = new FullQualifiedName( "nc.PersonSex" );
    private static FullQualifiedName RACE_FQN           = new FullQualifiedName( "nc.PersonRace" );
    private static FullQualifiedName ETHNICITY_FQN      = new FullQualifiedName( "nc.PersonEthnicity" );
    private static FullQualifiedName DOB_FQN            = new FullQualifiedName( "nc.PersonBirthDate" );
    private static FullQualifiedName IDENTIFICATION_FQN = new FullQualifiedName( "nc.SubjectIdentification" );
    private static FullQualifiedName SSN_FQN            = new FullQualifiedName( "nc.ssn" );
    private static FullQualifiedName AGE_FQN            = new FullQualifiedName( "person.age" );
    private static FullQualifiedName XREF_FQN           = new FullQualifiedName( "justice.xref" );

    public static DelegatedStringSet getValuesAsSet( Map<UUID, DelegatedStringSet> entity, UUID id ) {
        return DelegatedStringSet.wrap( entity.containsKey( id )
                ? entity.get( id ).stream().filter( StringUtils::isNotBlank ).collect( Collectors.toSet() )
                : Sets.newHashSet() );
    }

    public static int valueIsPresent( Map<UUID, DelegatedStringSet> entity, UUID propertyTypeId ) {
        if ( !entity.containsKey( propertyTypeId )
                || entity.get( propertyTypeId ).stream().filter( StringUtils::isNotBlank ).count() == 0 )
            return 0;
        return 1;
    }

    public static DelegatedStringSet getFirstName(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( FIRST_NAME_FQN ) );
    }

    public static int getHasFirstName( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( FIRST_NAME_FQN ) );
    }

    public static DelegatedStringSet getMiddleName(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( MIDDLE_NAME_FQN ) );
    }

    public static int getHasMiddleName(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( MIDDLE_NAME_FQN ) );
    }

    public static DelegatedStringSet getLastName(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( LAST_NAME_FQN ) );
    }

    public static int getHasLastName( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( LAST_NAME_FQN ) );
    }

    public static DelegatedStringSet getSex(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( SEX_FQN ) );
    }

    public static int getHasSex( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( SEX_FQN ) );
    }

    public static DelegatedStringSet getRace(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( RACE_FQN ) );
    }

    public static int getHasRace( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( RACE_FQN ) );
    }

    public static DelegatedStringSet getEthnicity(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( ETHNICITY_FQN ) );
    }

    public static int getHasEthnicity( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( ETHNICITY_FQN ) );
    }

    public static DelegatedStringSet getDob(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( DOB_FQN ) );
    }

    public static int getHasDob( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( DOB_FQN ) );
    }

    public static DelegatedStringSet getIdentification(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( IDENTIFICATION_FQN ) );
    }

    public static int getHasIdentification(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( IDENTIFICATION_FQN ) );
    }

    public static DelegatedStringSet getSsn(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( SSN_FQN ) );
    }

    public static int getHasSsn( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( SSN_FQN ) );
    }

    public static DelegatedStringSet getAge(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( AGE_FQN ) );
    }

    public static int getHasAge( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( AGE_FQN ) );
    }

    public static DelegatedStringSet getXref(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getValuesAsSet( entity, fqnToIdMap.get( XREF_FQN ) );
    }

    public static int getHasXref( Map<UUID, DelegatedStringSet> entity, Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return valueIsPresent( entity, fqnToIdMap.get( XREF_FQN ) );
    }

    public static DelegatedStringSet getDobStrs(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        DelegatedStringSet dobStrings = getValuesAsSet( entity, fqnToIdMap.get( DOB_FQN ) );
        if ( dobStrings.isEmpty() ) { return dobStrings; }
        DelegatedStringSet values = new DelegatedStringSet( Sets.newHashSet() );
        for ( String dobUnparsed : dobStrings ) {
            if ( dobUnparsed != null ) {
                if ( StringUtils.isEmpty( dobUnparsed ) ) { values.add( "" ); } else {
                    try {
                        LocalDate dt = LocalDate.parse( dobUnparsed );
                        String dobParsed = dd.format( dt.getDayOfMonth() ) + dd.format( dt.getMonthValue() ) + String
                                .valueOf( dt.getYear() );
                        values.add( dobParsed );
                    } catch ( Exception e ) {
                        logger.error( "Unable to parse date string" );
                    }
                }
            }
        }
        return values;
    }

}
