/*
 * Copyright (C) 2020. OpenLattice, Inc.
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
package com.openlattice.users.export

import com.auth0.json.mgmt.jobs.Job
import com.dataloom.mappers.ObjectMappers
import okhttp3.*
import org.slf4j.LoggerFactory

private const val JOBS_PATH = "api/v2/jobs"
private const val CONTENT_TYPE_APPLICATION_JSON = "application/json"

class UserExportEntity(private val client: OkHttpClient, private val baseUrl: HttpUrl, private val apiToken: String) {
    private val mapper = ObjectMappers.getJsonMapper()

    companion object {
        private val logger = LoggerFactory.getLogger(UserExportJobRequest::class.java)
    }

    /**
     * Submits a user export job to auth0.
     */
    fun submitExportJob(exportJob: UserExportJobRequest): Job {
        val url = baseUrl
                .newBuilder()
                .addPathSegments("$JOBS_PATH/users-exports")
                .build()
                .toString()

        val body = RequestBody.create(
                MediaType.parse(CONTENT_TYPE_APPLICATION_JSON), mapper.writeValueAsBytes(exportJob)
        )

        val request = Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer $apiToken")
                .addHeader("Content-Type", CONTENT_TYPE_APPLICATION_JSON)
                .post(body)
                .build()

        try {
            val response = client.newCall(request).execute()
            return mapper.readValue(response.body()?.bytes(), Job::class.java)
        } catch (ex: Exception) {
            logger.info("Encountered exception $ex when submitting export job $exportJob.")
            throw ex
        }
    }

    /**
     * Retrieves a job result from auth0 by [jobId].
     */
    fun getJob(jobId: String): UserExportJobResult {
        val url = baseUrl
                .newBuilder()
                .addPathSegments("$JOBS_PATH/$jobId")
                .build()
                .toString()

        val request = Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer $apiToken")
                .addHeader("Content-Type", CONTENT_TYPE_APPLICATION_JSON)
                .get()
                .build()

        try {
            val response = client.newCall(request).execute()
            return mapper.readValue(response.body()?.bytes(), UserExportJobResult::class.java)
        } catch (ex: Exception) {
            logger.info("Encountered exception $ex when trying to get export job $jobId.")
            throw ex
        }
    }
}