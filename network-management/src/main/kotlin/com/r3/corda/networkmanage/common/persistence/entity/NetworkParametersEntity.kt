package com.r3.corda.networkmanage.common.persistence.entity

import com.r3.corda.networkmanage.common.utils.SignedNetworkParameters
import net.corda.core.internal.DigitalSignatureWithCert
import net.corda.core.internal.SignedDataWithCert
import net.corda.core.serialization.SerializedBytes
import net.corda.core.serialization.deserialize
import net.corda.nodeapi.internal.crypto.X509CertificateFactory
import net.corda.nodeapi.internal.network.NetworkParameters
import org.hibernate.annotations.CreationTimestamp
import java.time.Instant
import javax.persistence.*

@Entity
@Table(name = "network_parameters", indexes = arrayOf(Index(name = "IDX_NET_PARAMS_HASH", columnList = "hash")))
class NetworkParametersEntity(
        @Id
        @Column(name = "hash", length = 64, unique = true)
        val parametersHash: String,

        @CreationTimestamp
        val created: Instant = Instant.now(),

        @Lob
        @Column(name = "parameters_bytes")
        val parametersBytes: ByteArray,

        // Both of the fields below are nullable, because of the way we sign network map data. NetworkParameters can be
        // inserted into database without signature. Then signing service will sign them.
        @Lob
        @Column(name = "signature")
        val signature: ByteArray?,

        @Lob
        @Column(name = "certificate")
        val certificate: ByteArray?
) {
    val isSigned: Boolean get() = certificate != null && signature != null

    fun toNetworkParameters(): NetworkParameters = parametersBytes.deserialize()

    fun toSignedNetworkParameters(): SignedNetworkParameters {
        if (certificate == null || signature == null) throw IllegalStateException("Network parameters entity is not signed: $parametersHash")
        return SignedDataWithCert(
                SerializedBytes(parametersBytes),
                DigitalSignatureWithCert(X509CertificateFactory().generateCertificate(certificate.inputStream()), signature)
        )
    }
}
