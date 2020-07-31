package net.corda.core.internal.messaging

import net.corda.core.internal.AttachmentTrustInfo
import net.corda.core.messaging.CordaRPCOps

/**
 * Contains internal RPC functions that should not be publicly exposed in [CordaRPCOps]
 */
interface InternalCordaRPCOps : CordaRPCOps {

    /** Dump all the current flow checkpoints as JSON into a zip file in the node's log directory. */
    fun dumpCheckpoints()

    /** Dump all the current flow checkpoints, alongside with the node's main jar, all CorDapps and driver jars
     * into a zip file in the node's log directory. */
    fun debugCheckpoints()

    /** Get all attachment trust information */
    val attachmentTrustInfos: List<AttachmentTrustInfo>
}