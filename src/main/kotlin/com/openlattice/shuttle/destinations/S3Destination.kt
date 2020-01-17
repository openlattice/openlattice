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

package com.openlattice.shuttle.destinations

import com.openlattice.data.*
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(S3Destination::class.java)


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class S3Destination(
        private val dataApi: DataApi,
        s3Api: S3Api,
        dataIntegrationApi: DataIntegrationApi
) : BaseS3Destination(s3Api, dataIntegrationApi) {
    override fun createAssociations(entities: Set<DataEdgeKey>): Long {
        return dataApi.createEdges(entities).toLong()
    }
}