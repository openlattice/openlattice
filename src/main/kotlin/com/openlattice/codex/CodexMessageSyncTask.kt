package com.openlattice.codex

import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.notifications.sms.SmsInformationKey
import com.openlattice.organizations.processors.UpdateSmsInformationLastSyncEntryProcessor
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

private val logger = LoggerFactory.getLogger(CodexMessageSyncTask::class.java)

class CodexMessageSyncTask : HazelcastFixedRateTask<CodexMessageSyncTaskDependencies> {

    override fun runTask() {
        logger.info("About to run CodexMessageSyncTask")
        val dependencies = getDependency()
        val smsInformationMapstore = HazelcastMap.SMS_INFORMATION.getMap(dependencies.hazelcast)
        val smsInformation = smsInformationMapstore.values.toSet()

        smsInformation.forEach { smsDetails ->
            val organizationId = smsDetails.organizationId
            val phoneNumber = smsDetails.phoneNumber
            val lastSync = smsDetails.lastSync

            val newLastSync = dependencies.codexService.integrateMessagesFromTwilioAfterLastSync(
                    organizationId,
                    phoneNumber,
                    lastSync
            )

            smsInformationMapstore.executeOnKey(
                    SmsInformationKey(phoneNumber, organizationId),
                    UpdateSmsInformationLastSyncEntryProcessor(newLastSync)
            )

            logger.info("Updated lastSync for org {} with phone number {} to {}", organizationId, phoneNumber, lastSync)

        }
        logger.info("Finished running CodexMessageSyncTask")
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getPeriod(): Long {
        return 1_000 * 60
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun getName(): String {
        return Task.CODEX_MESSAGE_SYNC_TASK.name
    }

    override fun getDependenciesClass(): Class<out CodexMessageSyncTaskDependencies> {
        return CodexMessageSyncTaskDependencies::class.java
    }
}