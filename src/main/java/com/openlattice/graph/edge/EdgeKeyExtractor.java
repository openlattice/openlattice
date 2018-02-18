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

package com.openlattice.graph.edge;

import com.openlattice.graph.core.Neighborhood;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EdgeKeyExtractor extends ValueExtractor<Neighborhood, String> {
    @Override public void extract(
            Neighborhood target, String argument, ValueCollector collector ) {

//        switch ( argument ) {
//            case "srcEntityKeyId":
//                collector.addObject( target.getSrcEntityKeyId() );
//            case "dstTypeId":
//                target.getDstTypeIds().forEach( collector::addObject );
//            case "dstSetId":
//                target.getDstEntityKeyIds().forEach( collector::addObject );
//            case "dstSyncId":
//            case "srcTypeId":
//            case "srcSetId":
//            case "srcSyncId":
//            case "edgeTypeId":
//            default:
//        }

    }
}
