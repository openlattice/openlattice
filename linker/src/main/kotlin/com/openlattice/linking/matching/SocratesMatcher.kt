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

package com.openlattice.linking.matching

import com.openlattice.data.EntityDataKey
import com.openlattice.linking.Matcher
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.util.ModelSerializer
import org.deeplearning4j.util.ModelSerializer.restoreMultiLayerNetwork
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.lang.ThreadLocal.withInitial
import java.util.*
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SocratesMatcher( model: MultiLayerNetwork ) : Matcher {
    private var inputStreamSupplier = memoizeModelAsStream(model)

    //            Thread.currentThread().contextClassLoader.getResourceAsStream("model.bin") }
    private val model: ThreadLocal<MultiLayerNetwork> = withInitial {
        restoreMultiLayerNetwork(inputStreamSupplier.get())
    }

    override fun updateMatchingModel(model: MultiLayerNetwork)  {
        inputStreamSupplier = memoizeModelAsStream(model)
    }

    private fun memoizeModelAsStream(model: MultiLayerNetwork) : Supplier<InputStream> {
        val outputStream  = ByteArrayOutputStream()
        ModelSerializer.writeModel(model, outputStream,true )
        return  Supplier { ByteArrayInputStream( outputStream.toByteArray() ) }
    }

    override fun match(
            block: Pair<EntityDataKey, Map<EntityDataKey, Map<UUID, Set<Any>>>>
    ): Pair<EntityDataKey, MutableMap<EntityDataKey, Map<EntityDataKey, Double>>> {
        val model = model.get()

        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }



}