package net.corda.flow.necromancer

import co.paralleluniverse.fibers.DefaultFiberScheduler
import co.paralleluniverse.fibers.Fiber
import com.sun.tools.attach.VirtualMachine
import net.corda.core.node.ServiceHub
import net.corda.core.serialization.internal.CheckpointSerializationDefaults
import net.corda.core.serialization.internal.SerializationEnvironment
import net.corda.core.serialization.internal.checkpointDeserialize
import net.corda.core.serialization.internal.nodeSerializationEnv
import net.corda.node.services.statemachine.FlowStateMachineImpl
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
import java.io.File
import java.lang.management.ManagementFactory
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.zip.ZipInputStream
import kotlin.streams.asSequence

//fun main(args: Array<String>) {
//    val runtimeMxBean = ManagementFactory.getRuntimeMXBean()
//    val arguments = runtimeMxBean.inputArguments
//    println(arguments)
//
//    val flowLoader = FlowLoader.instance!!
//    flowLoader.useArchive(Paths.get("/home/r3/checkpoints_debug.zip")) {
//        val cl = loadClass("org.slf4j.LoggerFactory")
//        val fl = loadClass("org.slf4j.helpers.NOPLogger")
//        val fiberFile = fibers().first()
//        val fib = fiber(fiberFile) as Fiber<*>
//        Fiber.unparkDeserialized(fib, DefaultFiberScheduler.getInstance())
//        val cls2 = fib.javaClass
//        println(cls2)
//
//    }
//}

class Bar(archivePath: Path) : AutoCloseable {
    companion object {
        private val quasarAgentArgs = """x(antlr**;bftsmart**;co.pa
            ralleluniverse**;com.codahale**;com.esotericsoftware**;com.fasterxml*
            *;com.google**;com.ibm**;com.intellij**;com.jcabi**;com.nhaarman**;co
            m.opengamma**;com.typesafe**;com.zaxxer**;de.javakaffee**;groovy**;gr
            oovyjarjarantlr**;groovyjarjarasm**;io.atomix**;io.github**;io.netty*
            *;jdk**;kotlin**;net.corda.djvm**;djvm**;net.bytebuddy**;net.i2p**;or
            g.apache**;org.bouncycastle**;org.codehaus**;org.crsh**;org.dom4j**;o
            rg.fusesource**;org.h2**;org.hibernate**;org.jboss**;org.jcp**;org.jo
            da**;org.objectweb**;org.objenesis**;org.slf4j**;org.w3c**;org.xml**;
            org.yaml**;reflectasm**;rx**;org.jolokia**;com.lmax**;picocli**;liqui
            base**;com.github.benmanes**;org.json**;org.postgresql**;nonapi.io.gi
            thub.classgraph**)l(net.corda.djvm.**;net.corda.core.serialization.in
            ternal.**)
        """.replace("\n", "").replace(" ", "")

        private fun startQuasarAgent(archivePath: Path, temporaryDir: Path) {
            synchronized(this) {
                val quasarJar = Files.list(temporaryDir.resolve("lib")).asSequence().firstOrNull {
                    it.fileName.toString().startsWith("quasar-core")
                } ?: throw RuntimeException("Quasar jar not found in $archivePath")
                val nameOfRunningVM = ManagementFactory.getRuntimeMXBean().name
                val pid = nameOfRunningVM.substring(0, nameOfRunningVM.indexOf('@'))
                val vm = VirtualMachine.attach(pid)
                vm.loadAgent(quasarJar.toString(), quasarAgentArgs)
                vm.detach()
            }
        }

        private fun extract(archivePath: Path, destination: Path) {
            val buffer = ByteArray(0x10000)
            ZipInputStream(Files.newInputStream(archivePath)).use { zis ->
                var zipEntry = zis.nextEntry
                while (zipEntry != null) {
                    val newFile: Path = destination.resolve(zipEntry.name)
                    Files.createDirectories(newFile.parent)
                    Files.newOutputStream(newFile).use { outputStream ->
                        while (true) {
                            val read = zis.read(buffer)
                            if (read < 0) break
                            outputStream.write(buffer, 0, read)
                        }
                    }
                    zipEntry = zis.nextEntry
                }
                zis.closeEntry()
            }
        }

        private fun deletePath(path: Path?) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map { obj: Path -> obj.toFile() }
                    .forEach { obj: File -> obj.delete() }
        }
    }

    val temporaryDir: Path
    val processBuilder: JavaProcessBuilder

    init {
        temporaryDir = Files.createTempDirectory("flowLoader")
        extract(archivePath, temporaryDir)
        val classes = sequenceOf(
                Bootstrapper::class.java.name,
                Bootstrapper.Companion::class.java.name,
                Bootstrapper.Companion::class.java.name + "\$main\$\$inlined\$newProxyObject\$1",
                Bootstrapper.Companion::class.java.name + "\$main\$\$inlined\$newProxyObject\$2",
                Bootstrapper.Companion::class.java.name + "\$newProxyObject\$1"
        )
        for (cls in classes) {
            val resourceName = cls.replace('.', '/') + ".class"
            val destination = temporaryDir.resolve("classes").resolve(resourceName)
            Files.createDirectories(temporaryDir.resolve("classes").resolve(resourceName).parent)
            Files.copy(javaClass.classLoader.getResource(resourceName)!!.openStream(), destination)
        }
        val jars = (Files.list(temporaryDir.resolve("lib")).asSequence() +
                Files.list(temporaryDir.resolve("drivers")).asSequence() +
                Files.list(temporaryDir.resolve("cordapps")).asSequence()
                ).filter {
                    it.fileName.toString().endsWith(".jar")
                }.toList()

        processBuilder = JavaProcessBuilder(Bootstrapper::class.java.name).apply {
            classPath = (jars.asSequence() + sequenceOf(temporaryDir.resolve("classes"))).joinToString(System.getProperty("path.separator"))
            jvmArgs = listOf("-javaagent:" + jars.first { it.fileName.toString().startsWith("quasar-core") })
        }
    }

    val fibers: Sequence<String>
        get() = Files.list(temporaryDir.resolve("fibers")).asSequence()
                .map { it.fileName.toString() }
                .filter { it.endsWith(".fiber") }

    var debugPort: Short? = null
    var suspend = false

    fun revive(fiberName: String) {
        processBuilder.properties.setProperty("net.corda.flow.necromancer.tmp.dir", temporaryDir.toString())
        processBuilder.cliArgs = arrayOf(fiberName)
        debugPort?.let {
            processBuilder.jvmArgs =
                    (processBuilder.jvmArgs.asSequence() +
                            "-agentlib:jdwp=transport=dt_socket,server=y,suspend=${if (suspend) 'y' else 'n'},address=$it").toList()
        }
        val process = processBuilder.exec {
            inheritIO()
        }
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() = process.destroy()
        })
        process.waitFor()
    }

    override fun close() {
        deletePath(temporaryDir)
    }
}

class Bootstrapper {
    companion object {
        private inline fun <reified T> newProxyObject(crossinline methodInvocation: (proxy: Any?, method: Method, args: Array<Any?>?) -> Any) =
                Proxy.newProxyInstance(T::class.javaClass.classLoader, arrayOf<Class<*>>(T::class.java)) { proxy, method, args ->
                    methodInvocation(proxy, method, args)
                } as T

        @JvmStatic
        fun main(args: Array<String>) {
            val classLoader = Bootstrapper::class.java.classLoader
            nodeSerializationEnv = SerializationEnvironment.with(
                    SerializationFactoryImpl().apply {
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

            val nodeServicesContext = newProxyObject<NodeServicesContext> { _: Any?, _: Method, _: Array<Any?>? ->
                TODO("Not yet implemented")
            }

            val serviceHub = newProxyObject<ServiceHub> { _: Any?, _: Method, _: Array<Any?>? ->
                TODO("Not yet implemented")
            }

            val checkpointSerializationContext = CheckpointSerializationDefaults.CHECKPOINT_CONTEXT.withTokenContext(
                    CheckpointSerializeAsTokenContextImpl(
                            nodeServicesContext,
                            CheckpointSerializationDefaults.CHECKPOINT_SERIALIZER,
                            CheckpointSerializationDefaults.CHECKPOINT_CONTEXT,
                            serviceHub
                    )
            )
            val temporaryDir = Paths.get(System.getProperty("net.corda.flow.necromancer.tmp.dir"))
            for (fiberName in args) {
                val fiber = Files.readAllBytes(temporaryDir.resolve("fibers/$fiberName"))
                        .checkpointDeserialize(checkpointSerializationContext) as FlowStateMachineImpl<*>
                Fiber.unparkDeserialized(fiber, DefaultFiberScheduler.getInstance())
            }
        }
    }
}

fun main(args: Array<String>) {
    val runtimeMxBean = ManagementFactory.getRuntimeMXBean()
    val arguments = runtimeMxBean.inputArguments
    println(arguments)

    val p = Paths.get("/home/r3/checkpoints_debug.zip")

    Bar(p).use { b ->
        b.debugPort = 4444
        b.suspend = true
        val fiberName = b.fibers.first()
        b.revive(fiberName)
    }
}