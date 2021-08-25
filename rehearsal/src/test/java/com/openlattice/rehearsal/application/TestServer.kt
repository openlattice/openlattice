/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
package com.openlattice.rehearsal.application

import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import org.apache.commons.lang3.StringUtils
import org.springframework.context.annotation.AnnotationConfigApplicationContext

class TestServer(vararg pods: Class<*>) {
    internal val context = AnnotationConfigApplicationContext()

    init {
        this.context.register(*pods)
    }

    fun sprout(vararg activeProfiles: String) {
        var awsProfile = false
        var localProfile = false
        for (profile in activeProfiles) {
            if (StringUtils.equals(ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE, profile)) {
                awsProfile = true
            }

            if (StringUtils.equals(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE, profile)) {
                localProfile = true
            }

            context.environment.addActiveProfile(profile)
        }

        if (!awsProfile && !localProfile) {
            context.environment.addActiveProfile(ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE)
        }

        context.refresh()
    }
}