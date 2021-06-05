package com.openlattice.graph.processing

class BaseGraphHandler<T> {

    fun hasCycle(graph: MutableMap<T, MutableSet<T>>):Boolean {
        return hasCycle(graph, getRoots(graph))
    }

   fun hasCycle(graph: MutableMap<T, MutableSet<T>>, initRoots: MutableSet<T>):Boolean {
       var roots = initRoots.toMutableSet()
       val remain = graph.toMutableMap()

       while(!roots.isEmpty()) {
           roots.forEach {
               remain.remove(it)
           }
           roots = getRoots(remain)
       }

       return !remain.isEmpty()
   }

   private fun getRoots(graph : MutableMap<T, MutableSet<T>>): MutableSet<T> {
       val rootCandidates = graph.keys.toMutableSet()

       graph.values.toMutableList().flatten().forEach {
           rootCandidates.remove(it)
       }

       return rootCandidates
   }
}