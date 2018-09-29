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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import com.google.common.io.Resources;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public enum PersonMetric {
    FIRST_NAME_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getFirstName( e, map ) ) ),
    FIRST_NAME_METAPHONE( metaphone( ( e, map ) -> PersonProperties.getFirstName( e, map ) ) ),
    FIRST_NAME_METAPHONE_ALT( metaphoneAlternate( ( e, map ) -> PersonProperties.getFirstName( e, map ) ) ),
    FIRST_NAME_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasFirstName( e, map ) ) ),
    FIRST_NAME_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasFirstName( e, map ) ) ),
    FIRST_NAME_LHS_PROBA( lhsdouble( ( e, map) -> PersonProperties.getFirstProba (e, map ) ) ),
    FIRST_NAME_RHS_PROBA( rhsdouble( ( e, map) -> PersonProperties.getFirstProba (e, map ) ) ),

    MIDDLE_NAME_STRING( jaroWinkler( ( e, map ) -> DelegatedStringSet.wrap( Sets.newHashSet() ) ) ),
    MIDDLE_NAME_METAPHONE( metaphone( ( e, map ) -> DelegatedStringSet.wrap( Sets.newHashSet() ) ) ),
    MIDDLE_NAME_METAPHONE_ALT( metaphoneAlternate( ( e, map ) -> DelegatedStringSet.wrap( Sets.newHashSet() ) ) ),
    MIDDLE_NAME_LHS_PRESENCE( lhs( ( e, map ) -> 0 ) ),
    MIDDLE_NAME_RHS_PRESENCE( rhs( ( e, map ) -> 0 ) ),

    LAST_NAME_STRINGG( jaroWinkler( ( e, map ) -> PersonProperties.getLastName( e, map ) ) ),
    LAST_NAME_METAPHONE( metaphone( ( e, map ) -> PersonProperties.getLastName( e, map ) ) ),
    LAST_NAME_METAPHONE_ALT( metaphoneAlternate( ( e, map ) -> PersonProperties.getLastName( e, map ) ) ),
    LAST_NAME_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasLastName( e, map ) ) ),
    LAST_NAME_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasLastName( e, map ) ) ),
    LAST_NAME_LHS_PROBA( lhsdouble( ( e, map) -> PersonProperties.getLastProba (e, map ) ) ),
    LAST_NAME_RHS_PROBA( rhsdouble( ( e, map) -> PersonProperties.getLastProba (e, map ) ) ),

    SEX_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getSex( e, map ) ) ),
    SEX_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasSex( e, map ) ) ),
    SEX_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasSex( e, map ) ) ),

    DOB_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getDob( e, map ) ) ),
    DOB_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasDob( e, map ) ) ),
    DOB_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasDob( e, map ) ) ),
    DOB_DIFF( dobStr() ),

    RACE_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getRace( e, map ) ) ),
    RACE_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasRace( e, map ) ) ),
    RACE_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasRace( e, map ) ) ),

    ETHNICITY_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getEthnicity( e, map ) ) ),
    ETHNICITY_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasEthnicity( e, map ) ) ),
    ETHNICITY_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasEthnicity( e, map ) ) ),

    SSN_STRING( jaroWinkler( ( e, map ) -> PersonProperties.getSsn( e, map ) ) ),
    SSN_LHS_PRESENCE( lhs( ( e, map ) -> PersonProperties.getHasSsn( e, map ) ) ),
    SSN_RHS_PRESENCE( rhs( ( e, map ) -> PersonProperties.getHasSsn( e, map ) ) );


    private static final PersonMetric[]    metrics         = PersonMetric.values();
    private static final Set<PersonMetric> metricsList     = Sets.newHashSet( PersonMetric.values() );
    private static final DoubleMetaphone   doubleMetaphone = new DoubleMetaphone();

    private final MetricExtractor metric;

    PersonMetric( MetricExtractor metric ) {
        this.metric = metric;
    }

    private double extract(
            Map<UUID, DelegatedStringSet> lhs,
            Map<UUID, DelegatedStringSet> rhs,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        return this.metric.extract( lhs, rhs, fqnToIdMap );
    }

    public static Double[] distance(
            Map<UUID, DelegatedStringSet> lhs,
            Map<UUID, DelegatedStringSet> rhs,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        Double[] result = new Double[ metrics.length ];
        metricsList.parallelStream().forEach( m -> {
            result[ m.ordinal() ] = m.extract( lhs, rhs, fqnToIdMap );
        } );
        return result;
    }

    public static double[] pDistance(
            Map<UUID, DelegatedStringSet> lhs,
            Map<UUID, DelegatedStringSet> rhs,
            Map<FullQualifiedName, UUID> fqnToIdMap ) {
        double[] result = new double[ metrics.length ];
        metricsList.parallelStream().forEach( m -> {
            result[ m.ordinal() ] = m.extract( lhs, rhs, fqnToIdMap );
        } );
        return result;
    }

    public static MetricExtractor lhs( BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, Integer> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> extractor.apply( lhs, fqnToIdMap );
    }

    public static MetricExtractor rhs( BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, Integer> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> extractor.apply( rhs, fqnToIdMap );
    }

    public static MetricExtractor lhsdouble( BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, Double> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> extractor.apply( lhs, fqnToIdMap );
    }

    public static MetricExtractor rhsdouble( BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, Double> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> extractor.apply( rhs, fqnToIdMap );
    }


    public static MetricExtractor jaroWinkler(
            BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, DelegatedStringSet> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> getMaxStringDistance( extractor.apply( lhs, fqnToIdMap ),
                extractor.apply( rhs, fqnToIdMap ),
                false,
                true );
    }

    public static MetricExtractor metaphone(
            BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, DelegatedStringSet> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> getMaxStringDistance( extractor.apply( lhs, fqnToIdMap ),
                extractor.apply( rhs, fqnToIdMap ),
                true,
                false );
    }

    public static MetricExtractor metaphoneAlternate(
            BiFunction<Map<UUID, DelegatedStringSet>, Map<FullQualifiedName, UUID>, DelegatedStringSet> extractor ) {
        return ( lhs, rhs, fqnToIdMap ) -> getMaxStringDistance( extractor.apply( lhs, fqnToIdMap ),
                extractor.apply( rhs, fqnToIdMap ),
                true,
                true );
    }

    public static MetricExtractor dobStr() {
        return ( lhs, rhs, fqnToIdMap ) -> {
            DelegatedStringSet lhsDob = PersonProperties.getDobStrs( lhs, fqnToIdMap );
            DelegatedStringSet rhsDob = PersonProperties.getDobStrs( rhs, fqnToIdMap );
            double bestValue = 0;
            for (String dob1 : lhsDob) {
                for (String dob2 : rhsDob) {
                    double val = ( 8 - StringUtils.getLevenshteinDistance( dob1, dob2 ) ) / 8.0;
                    if (val > bestValue) bestValue = val;
                }
            }
            return bestValue;
        };
    }

    public static double getMaxStringDistance(
            DelegatedStringSet lhs,
            DelegatedStringSet rhs,
            boolean useMetaphone,
            boolean alternate ) {
        double max = 0;
        for ( String s1 : lhs ) {
            for ( String s2 : rhs ) {
                double difference = getStringDistance( s1.toLowerCase(), s2.toLowerCase(), useMetaphone, alternate );
                if ( difference > max ) { max = difference; }
            }
        }
        return max;
    }

    public static double getStringDistance( String lhs, String rhs, boolean useMetaphone, boolean alternate ) {
        if ( lhs == null ) {
            lhs = "";
        }

        if ( rhs == null ) {
            rhs = "";
        }

        if ( useMetaphone ) {
            if ( StringUtils.isNotBlank( lhs ) ) {
                lhs = doubleMetaphone.doubleMetaphone( lhs, alternate );
            }
            if ( StringUtils.isNotBlank( rhs ) ) {
                rhs = doubleMetaphone.doubleMetaphone( rhs, alternate );
            }
        }

        return StringUtils.getJaroWinklerDistance( lhs, rhs );
    }

}