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

import com.google.common.base.Charsets
import java.io.IOException
import java.lang.reflect.Type
import org.apache.commons.io.IOUtils
import org.junit.Assert
import org.slf4j.LoggerFactory
import retrofit2.Call
import retrofit2.CallAdapter
import retrofit2.Retrofit

class ThrowingCallAdapterFactory : CallAdapter.Factory() {
    companion object {
        private val logger = LoggerFactory.getLogger(ThrowingCallAdapterFactory::class.java)
    }

    override fun get(returnType: Type, annotations: Array<Annotation>, retrofit: Retrofit): CallAdapter<*, *> =
            object : CallAdapter<Any, Any> {
                override fun responseType(): Type {
                    return returnType
                }

                override fun adapt(call: Call<Any>): Any? {
                    return try {
                        val response = call.execute()
                        if (response.code() >= 400) {
                            val errorBody = IOUtils.toString(response.errorBody()!!.byteStream(), Charsets.UTF_8)
                            logger.error("Call failed with code {} and message {} and error body {}",
                                    response.code(),
                                    response.message(),
                                    errorBody)
                            throw GeneralException(errorBody)
                        }
                        response.body()
                    } catch (e: IOException) {
                        logger.error("Call failed.", e)
                        null
                    }
                }

            }
}

class GeneralException(message: String) : java.lang.Exception(message)

internal fun assertException(fqn: () -> (Any), expectedMsg: String) {
    try {
        fqn()
        Assert.fail("Should have thrown Exception but did not!")
    } catch (e: Exception) {
        Assert.assertTrue(e.message!!.contains(expectedMsg, true))
    }
}