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

package com.openlattice.rehearsal

import com.openlattice.authentication.AuthenticationTest
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import com.openlattice.shuttle.MissionControl
import com.openlattice.shuttle.ShuttleCli
import com.openlattice.shuttle.main
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.io.File
import java.util.*

/**
 * Helper functions for integrating data before running tests.
 */
open class SetupTestData : MultipleAuthenticatedUsersBase() {
    companion object {
        private const val DATA_FOLDER = "data"
        private const val FLIGHT_FOLDER = "flights"

        init {
            MissionControl.continueAfterSuccess()
        }

        /**
         * Import datasets via Shuttle
         * @param
         */
        fun importDataSet(flightFileName: String, dataFileName: String) {
            loginAs("admin")
            val tokenAdmin = AuthenticationTest.getAuthentication(authOptions).credentials

            val flightFile = File(Thread.currentThread().contextClassLoader.getResource(FLIGHT_FOLDER).file,
                    flightFileName).absolutePath
            val dataFile = File(Thread.currentThread().contextClassLoader.getResource(DATA_FOLDER).file, dataFileName)
                    .absolutePath
            val email = getUserInfo(SetupEnvironment.admin).email

            main(arrayOf(
                    "-${ShuttleCli.FLIGHT}=$flightFile",
                    "-${ShuttleCli.CSV}=$dataFile",
                    "-${ShuttleCli.ENVIRONMENT}=LOCAL",
                    "-${ShuttleCli.TOKEN}=$tokenAdmin",
                    "-${ShuttleCli.CREATE}=$email"))
        }

        /**
         * Indicates whether the [com.openlattice.linking.RealtimeLinkingService] is finished for entitysets
         */
        fun checkLinkingFinished( importedGeneralPersonFqns: Set<String>): Boolean {
            val finishedEntitySets = realtimeLinkingApi.linkingFinishedEntitySets
            val finished = importedGeneralPersonFqns.all { finishedEntitySets.contains(entitySetsApi.getEntitySetId(it)) }

            logger.info("Linking is finished:{} with imported entity sets: {}", finished, importedGeneralPersonFqns)
            return finished
        }
    }

}