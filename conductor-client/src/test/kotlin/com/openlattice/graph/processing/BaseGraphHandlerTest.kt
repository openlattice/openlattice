package com.openlattice.graph.processing

import org.junit.Assert
import org.junit.Test

class BaseGraphHandlerTest {
    private val handler = BaseGraphHandler<Int>()

    @Test
    fun testHasNoCycle() {
        val graph = mutableMapOf(
                1 to mutableSetOf(3),
                2 to mutableSetOf(3, 4),
                3 to mutableSetOf(4, 5),
                4 to mutableSetOf(5, 6))

        Assert.assertFalse(handler.hasCycle(graph))
    }

    @Test
    fun testHasOneCycle() {
        val graph = mutableMapOf(
                1 to mutableSetOf(2, 3),
                2 to mutableSetOf(1))

        Assert.assertTrue(handler.hasCycle(graph))
    }

    @Test
    fun testHasInnerCycle() {
        val graph = mutableMapOf(
                1 to mutableSetOf(2, 3),
                2 to mutableSetOf(4),
                3 to mutableSetOf(2),
                4 to mutableSetOf(3))

        Assert.assertTrue(handler.hasCycle(graph))
    }
}