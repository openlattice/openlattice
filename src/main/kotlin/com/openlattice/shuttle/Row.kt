package com.openlattice.shuttle

interface Row {
    fun <T> getAs(column: String): T
}