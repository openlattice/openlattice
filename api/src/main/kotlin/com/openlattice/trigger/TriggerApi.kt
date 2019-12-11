package com.openlattice.trigger

/**
 * Used for triggering things.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface TriggerApi {
    fun createTrigger(trigger: Trigger, apiKey: String): Boolean

    fun deleteTrigger(name: String, apiKey: String): Void?

    fun fireTrigger(name: String, apiKey: String): Void
}