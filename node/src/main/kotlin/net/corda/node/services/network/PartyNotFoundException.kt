package net.corda.node.services.network

import net.corda.core.CordaRuntimeException
import net.corda.core.identity.CordaX500Name

class PartyNotFoundException(message: String, val party: CordaX500Name) : CordaRuntimeException(message)