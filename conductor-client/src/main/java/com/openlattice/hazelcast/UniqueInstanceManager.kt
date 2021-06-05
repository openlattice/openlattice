package com.openlattice.hazelcast

import com.google.common.collect.Maps
import org.slf4j.LoggerFactory

class UniqueInstanceManager(private val clazz: Class<*>) {
    private val previousInstances = Maps.newConcurrentMap<String, Exception>()

    companion object {
        private val logger = LoggerFactory
                .getLogger(UniqueInstanceManager::class.java)
        private val SAFE = SucceededCheck()
    }

    interface InstanceChecker {
        fun check()
        fun ok(): Boolean
    }

    class SucceededCheck: InstanceChecker {
        override fun ok(): Boolean {
            return true;
        }

        override fun check() {
            // noop
        }

    }

    class FailedCheck(private val previous: Exception): InstanceChecker {
        override fun check() {
            throw IllegalStateException("This is a duplicate of a previous id, constructed here", previous)
        }

        override fun ok(): Boolean {
            return false
        }
    }

    fun checkInstance(key: String): InstanceChecker {
        val myInit = Exception("originally constructed here")
        val prev = previousInstances.putIfAbsent(key, myInit)
        return if (prev == null) {
            SAFE
        } else {
            logger.error("Duplicate instances of type {} with key {}", this.clazz.name, key)
            FailedCheck(prev)
        }
    }
}