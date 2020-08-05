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
import com.openlattice.shuttle.ShuttleCliOptions
import com.openlattice.shuttle.main
import org.junit.Test
import java.io.File

/**
 * Helper functions for integrating data before running tests.
 */
open class SetupTestData : MultipleAuthenticatedUsersBase() {
    companion object {
        private const val DATA_FOLDER = "data"
        private const val FLIGHT_FOLDER = "flights"
        private const val CONFIG_FOLDER = "config"

        init {
            MissionControl.continueAfterSuccess()
        }

        fun getDefaultShuttleArgs(): Array<String> {
            loginAs("admin");
            val tokenAdmin = AuthenticationTest.getAuthentication(authOptions).credentials
            val email = getUserInfo(SetupEnvironment.admin).email

            return arrayOf(
                    "-${ShuttleCliOptions.ENVIRONMENT}=LOCAL",
                    "-${ShuttleCliOptions.FETCHSIZE}=1000",
                    "-${ShuttleCliOptions.UPLOAD_SIZE}=1000",
                    "-${ShuttleCliOptions.TOKEN}=$tokenAdmin",
                    "-${ShuttleCliOptions.CREATE}=$email")
        }

        /**
         * Import datasets from CSV via Shuttle
         * @param flightFileName
         * @param dataFileName
         */
        fun importDataSetFromCSV(flightFileName: String, dataFileName: String) {
            val flightFile = File(Thread.currentThread().contextClassLoader.getResource(FLIGHT_FOLDER).file,
                    flightFileName).absolutePath
            val dataFile = File(Thread.currentThread().contextClassLoader.getResource(DATA_FOLDER).file, dataFileName)
                    .absolutePath
            val shuttleArgs : Array<String> = getDefaultShuttleArgs()

            main(shuttleArgs.plus(
                    arrayOf(
                            "-${ShuttleCliOptions.FLIGHT}=$flightFile",
                            "-${ShuttleCliOptions.CSV}=$dataFile"
                    )
            ))
        }

        /**
         * Import datasets from CSV via Shuttle
         * @param flightFileName
         * @param flightSql
         * @param dataConfiguration
         * @param dataConfigurationKey
         */
        fun importDataSetFromAtlas(flightFileName: String, flightSql: String, dataConfiguration: String, dataConfigurationKey: String) {
            val flightFile = File(Thread.currentThread().contextClassLoader.getResource(FLIGHT_FOLDER).file,
                    flightFileName).absolutePath
            val configFile = File(Thread.currentThread().contextClassLoader.getResource(CONFIG_FOLDER).file, dataConfiguration)
                        .absolutePath
            val shuttleArgs : Array<String> = getDefaultShuttleArgs()
            main(shuttleArgs.plus(
                    arrayOf(
                            "-${ShuttleCliOptions.FLIGHT}=$flightFile",
                            "-${ShuttleCliOptions.SQL}=$flightSql",
                            "-${ShuttleCliOptions.CONFIGURATION}=$configFile",
                            "-${ShuttleCliOptions.DATASOURCE}=$dataConfigurationKey"
                    )
            ))
        }


        /**
         * Indicates whether the [com.openlattice.linking.RealtimeLinkingService] is finished for entitysets
         */
        fun checkLinkingFinished(importedGeneralPersonFqns: Set<String>): Boolean {
            val finishedEntitySets = realtimeLinkingApi.linkingFinishedEntitySets
            val finished = importedGeneralPersonFqns.all { finishedEntitySets.contains(entitySetsApi.getEntitySetId(it)) }

            logger.info("Linking is finished:{} with imported entity sets: {}", finished, importedGeneralPersonFqns)
            return finished
        }
    }

    @Test
    fun doImport() {
        importDataSetFromCSV("socratesA.yaml", "testdata1.csv")
        importDataSetFromCSV("socratesB.yaml", "testdata2.csv")
    }


}