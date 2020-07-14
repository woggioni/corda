package net.corda.flow.necromancer

import co.paralleluniverse.fibers.DefaultFiberScheduler
import co.paralleluniverse.fibers.Fiber
import net.corda.core.node.ServiceHub
import net.corda.core.serialization.internal.CheckpointSerializationContext
import net.corda.core.serialization.internal.CheckpointSerializationDefaults
import net.corda.core.serialization.internal.SerializationEnvironment
import net.corda.core.serialization.internal.checkpointDeserialize
import net.corda.core.serialization.internal.nodeSerializationEnv
import net.corda.nodeapi.internal.lifecycle.NodeServicesContext
import net.corda.nodeapi.internal.rpc.client.AMQPClientSerializationScheme
import net.corda.nodeapi.internal.serialization.amqp.AMQPServerSerializationScheme
import net.corda.nodeapi.internal.serialization.kryo.KRYO_CHECKPOINT_CONTEXT
import net.corda.nodeapi.internal.serialization.kryo.KryoCheckpointSerializer
import net.corda.serialization.internal.AMQP_P2P_CONTEXT
import net.corda.serialization.internal.AMQP_RPC_CLIENT_CONTEXT
import net.corda.serialization.internal.AMQP_RPC_SERVER_CONTEXT
import net.corda.serialization.internal.AMQP_STORAGE_CONTEXT
import net.corda.serialization.internal.CheckpointSerializeAsTokenContextImpl
import net.corda.serialization.internal.SerializationFactoryImpl
import net.corda.serialization.internal.withTokenContext
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.nio.file.Files
import java.nio.file.Path
import kotlin.streams.asSequence

interface FlowNecromancer {
    fun listFibers() : Sequence<String>
    fun revive(fiberName : String)
}

class FlowNecromancerImpl(classLoader : ClassLoader, private val temporaryDir: Path) : FlowNecromancer {

    companion object {
        private inline fun <reified T> newProxyObject(classLoader : ClassLoader,
                                                      crossinline methodInvocation: (proxy: Any?, method: Method, args: Array<Any?>?) -> Any) =
                Proxy.newProxyInstance(classLoader, arrayOf<Class<*>>(T::class.java)) { proxy, method, args ->
                    methodInvocation(proxy, method, args)
                } as T

        private var serializationInitialized : Boolean = false
    }

    val checkpointSerializationContext : CheckpointSerializationContext

    init {
        Thread.currentThread().contextClassLoader = classLoader

        if(serializationInitialized == false) {
            synchronized(Companion) {
                if(serializationInitialized == false) {
                    nodeSerializationEnv = SerializationEnvironment.with(
                            SerializationFactoryImpl().apply
                            {
                                registerScheme(AMQPServerSerializationScheme(
                                        emptyList()))
                                registerScheme(AMQPClientSerializationScheme(
                                        emptyList()))
                            },
                            p2pContext = AMQP_P2P_CONTEXT.withClassLoader(classLoader = classLoader),
                            rpcServerContext = AMQP_RPC_SERVER_CONTEXT.withClassLoader(classLoader),
                            rpcClientContext = AMQP_RPC_CLIENT_CONTEXT.withClassLoader(classLoader),
                            storageContext = AMQP_STORAGE_CONTEXT.withClassLoader(classLoader),
                            checkpointSerializer = KryoCheckpointSerializer,
                            checkpointContext = KRYO_CHECKPOINT_CONTEXT.withClassLoader(classLoader)
                    )
                    serializationInitialized = true
                }
            }
        }

        val nodeServicesContext2 = Proxy.newProxyInstance(this::class.java.classLoader, arrayOf<Class<*>>(NodeServicesContext::class.java)) { proxy, method, args ->
            TODO("Not yet implemented")
        } as NodeServicesContext

        val nodeServicesContext = newProxyObject<NodeServicesContext>(classLoader) { _: Any?, _: Method, _: Array<Any?>? ->
            TODO("Not yet implemented")
        }

        val serviceHub = newProxyObject<ServiceHub>(classLoader) { _: Any?, _: Method, _: Array<Any?>? ->
            TODO("Not yet implemented")
        }

        val res = classLoader.loadClass("co.paralleluniverse.fibers.instrument.JavaAgent").getMethod("isActive").invoke(null) as Boolean

        check(res) {
            "Missing the '-javaagent' JVM argument. Make sure you run the tests with the Quasar java agent attached to your JVM."
        }
        checkpointSerializationContext = CheckpointSerializationDefaults.CHECKPOINT_CONTEXT.withTokenContext(
                CheckpointSerializeAsTokenContextImpl(
                        nodeServicesContext,
                        CheckpointSerializationDefaults.CHECKPOINT_SERIALIZER,
                        CheckpointSerializationDefaults.CHECKPOINT_CONTEXT,
                        serviceHub
                )
        )
    }

    override fun listFibers() = Files.list(temporaryDir.resolve("fibers")).asSequence()
            .map { it.fileName.toString() }
            .filter { it.endsWith(".fiber") }

    override fun revive(fiberName: String) {
        val fiber = Files.readAllBytes(temporaryDir.resolve("fibers/$fiberName")).checkpointDeserialize(checkpointSerializationContext) as Fiber<*>
        Fiber.unparkDeserialized(fiber, DefaultFiberScheduler.getInstance())
    }
}