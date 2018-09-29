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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.fasterxml.jackson.databind.MappingIterator;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;

public class PersonProperties {
    private static final Logger            logger             = LoggerFactory.getLogger( PersonProperties.class );
    private static final FullQualifiedName FIRST_NAME_FQN     = new FullQualifiedName( "nc.PersonGivenName" );
    private static final FullQualifiedName MIDDLE_NAME_FQN    = new FullQualifiedName( "nc.PersonMiddleName" );
    private static final FullQualifiedName LAST_NAME_FQN      = new FullQualifiedName( "nc.PersonSurName" );
    private static final FullQualifiedName SEX_FQN            = new FullQualifiedName( "nc.PersonSex" );
    private static final FullQualifiedName RACE_FQN           = new FullQualifiedName( "nc.PersonRace" );
    private static final FullQualifiedName ETHNICITY_FQN      = new FullQualifiedName( "nc.PersonEthnicity" );
    private static final FullQualifiedName DOB_FQN            = new FullQualifiedName( "nc.PersonBirthDate" );
    private static final FullQualifiedName IDENTIFICATION_FQN = new FullQualifiedName( "nc.SubjectIdentification" );
    private static final FullQualifiedName SSN_FQN            = new FullQualifiedName( "nc.ssn" );
    private static final FullQualifiedName AGE_FQN            = new FullQualifiedName( "person.age" );
    private static final FullQualifiedName XREF_FQN           = new FullQualifiedName( "justice.xref" );
    public static final Set<FullQualifiedName> FQNS = ImmutableSet.of( FIRST_NAME_FQN,
            MIDDLE_NAME_FQN,
            LAST_NAME_FQN,
            SEX_FQN,
            RACE_FQN,
            ETHNICITY_FQN,
            DOB_FQN,
            IDENTIFICATION_FQN,
            SSN_FQN,
            AGE_FQN,
            XREF_FQN );
    private static Map<String, Double> firstNameCounts;
    private static Map<String, Double> lastNameCounts;
    private static final DecimalFormat dd = new DecimalFormat( "00" );

    static {
        try {
            firstNameCounts = loadFile("firstnames.csv");
            lastNameCounts = loadFile("lastnames.csv");
        } catch (Exception e) {
            logger.info("Unable to load file!", e);
            firstNameCounts = null;
            lastNameCounts = null;
            throw new IllegalStateException(e);
        }
    }


    public static Map<String, Double> loadFile(String namefile) throws IOException {
        Map<String, Double> m = new ConcurrentHashMap<>();
        CsvMapper mapper = new CsvMapper();
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new AfterburnerModule());
        CsvSchema schema = mapper.schemaFor(Name.class).withHeader();
        MappingIterator<Name> iter = mapper.readerFor(Name.class).with(schema).readValues(Resources.getResource(namefile));
        iter.forEachRemaining(n -> {
            m.put(n.getName(), n.getProb());
        });
        return m;
    }


    public static DelegatedStringSet getValuesAsSet( Map<UUID, DelegatedStringSet> entity, UUID id ) {
        return DelegatedStringSet.wrap( entity.containsKey( id )
                ? entity.get( id ).stream().filter( StringUtils::isNotBlank ).collect( Collectors.toSet() )
                : Sets.newHashSet() );
    }

    public static int valueIsPresent( Map<UUID, DelegatedStringSet> entity, UUID propertyTypeId ) {
        if ( !entity.containsKey( propertyTypeId )
                || entity.get( propertyTypeId ).stream().filter( StringUtils::isNotBlank ).count() == 0 ) { return 0; }
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

    public static double getFirstProba(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getProba(entity, fqnToIdMap, true);
    }

    public static double getLastProba(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return getProba(entity, fqnToIdMap,false);
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Name {
        private final String name;
        private final double prob;

        @JsonCreator
        public Name(
                @JsonProperty("name") String name, @JsonProperty("prob") double prob) {
            this.name = name;
            this.prob = prob;
        }

        @JsonProperty("name")
        public String getName() {
            return name;
        }

        @JsonProperty("prob")
        public double getProb() {
            return prob;
        }

    }

    public static double getProba(
            Map<UUID, DelegatedStringSet> entity,
            Map<FullQualifiedName, UUID> fqnToIdMap ,
            boolean first) {
        final DelegatedStringSet name = first ? getFirstName(entity, fqnToIdMap) : getLastName(entity, fqnToIdMap);
        final Map<String, Double> NameCounts = first? firstNameCounts : lastNameCounts ;

        if (NameCounts.containsKey(name)) {
            return NameCounts.get(name);
        } else {
            return 0;
        }

    }
//        logger.info(proba);


}
