package net.corda.flow.necromancer

import co.paralleluniverse.fibers.DefaultFiberScheduler
import co.paralleluniverse.fibers.Fiber
import com.sun.tools.attach.VirtualMachine
import sun.misc.CompoundEnumeration
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.lang.management.ManagementFactory
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.net.URL
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.zip.ZipInputStream
import kotlin.Comparator
import kotlin.streams.asSequence

class FlowLoader(parent: ClassLoader) : ClassLoader(parent) {

    companion object {
        var instance: FlowLoader? = null
            private set

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

        private inline fun <reified T> newProxyObject(crossinline methodInvocation: (proxy: Any?, method: Method, args: Array<Any?>?) -> Any) =
                Proxy.newProxyInstance(T::class.javaClass.classLoader, arrayOf<Class<*>>(T::class.java)) { proxy, method, args ->
                    methodInvocation(proxy, method, args)
                } as T

        @JvmStatic
        fun main(args: Array<String>) {
            val runtimeMxBean = ManagementFactory.getRuntimeMXBean()
            val arguments = runtimeMxBean.inputArguments
            println(arguments)

//            val systemClassLoader = getSystemClassLoader()
//            val res = systemClassLoader.getResource("/net/corda/flow/necromancer/Runner.class")
//            val cls = getSystemClassLoader().loadClass(Runner::class.java.name)
//            throw java.lang.RuntimeException("KA-BOOM!!!")
            val archivePath = Paths.get("/home/r3/checkpoints_debug.zip")
            val flowLoader = getSystemClassLoader() as FlowLoader
            flowLoader.useArchive(archivePath) { flowNecomancer ->
                val fiberFile = flowNecomancer.listFibers().first()
                val fib = flowNecomancer.revive(fiberFile) as Fiber<*>
                Fiber.unparkDeserialized(fib, DefaultFiberScheduler.getInstance())
            }
        }
    }

    init {
        if (instance != null) {
            throw IllegalStateException("Only one instance of this class is allowed")
        } else {
            instance = this
        }
    }

    var jars : Iterable<FileSystem>? = null

    fun useArchive(archivePath: Path, cb : FlowLoader.(necromancer : FlowNecromancer) -> Unit) {
        val temporaryDir = Files.createTempDirectory("flowLoader")
        try {
            extract(archivePath, temporaryDir)
            startQuasarAgent(archivePath, temporaryDir)
            jars = (
                    Files.list(temporaryDir.resolve("lib")).asSequence() +
                            Files.list(temporaryDir.resolve("drivers")).asSequence() +
                            Files.list(temporaryDir.resolve("cordapps")).asSequence()
                    ).filter {
                        it.fileName.toString().endsWith(".jar")
                    }.map {
                        FileSystems.newFileSystem(it, null)
                    }.toList()
            cb(FlowNecromancerImpl(this, temporaryDir))
            jars = null
        } finally {
            deletePath(temporaryDir)
        }
    }

    override fun loadClass(name: String, resolve: Boolean): Class<*>? {
        synchronized(getClassLoadingLock(name)) {
            var c = findLoadedClass(name)
            if (c == null) {
                c = findClass(name)
                if (c == null) {
                    c = super.loadClass(name, resolve)
                }
            }
            if (c == null) {
                throw ClassNotFoundException(name)
            }
            else if (resolve) {
                resolveClass(c)
            }
            return c
        }
    }

    private fun readAllBytes(inputStream: InputStream): ByteArray {
        val result = ByteArrayOutputStream()
        val buffer = ByteArray(0x10000)
        while (true) {
            val read = inputStream.read(buffer, 0, buffer.size)
            if (read < 0) break
            result.write(buffer, 0, read)
        }
        return result.toByteArray()
    }

    override fun findClass(name: String): Class<*>? {
        val resourceName = name.replace('.', '/') + ".class"
        return when {
//            name.startsWith("net.corda.flow.necromancer") ||
            name.startsWith("adasda" + FlowLoader::class.java.name) ->
                getSystemClassLoader().getResourceAsStream(resourceName)
            else -> findResource(resourceName)?.let(URL::openStream)
        }?.use { inputStream ->
            val bytes = readAllBytes(inputStream)
            defineClass(name, bytes, 0, bytes.size)
        }
    }

    override fun findResource(name: String): URL? = findResources(name).let {
        if (it.hasMoreElements()) it.nextElement() else null
    }

    override fun getResource(name: String): URL? {
        return findResource(name) ?: parent.getResource(name)
    }

    override fun findResources(name: String): Enumeration<URL> = jars?.let { jars ->
            val it = jars.asSequence().map {
                it.getPath(name)
            }.filter {
                Files.exists(it)
            }.map {
                it.toUri().toURL()
            }.iterator()
            return object : Enumeration<URL> {
                override fun hasMoreElements(): Boolean = it.hasNext()
                override fun nextElement(): URL = it.next()
            }
        } ?: Collections.emptyEnumeration()

    override fun getResources(name: String): Enumeration<URL> {
        return CompoundEnumeration(arrayOf(findResources(name), parent?.getResources(name)))
    }

    fun appendToClassPathForInstrumentation(f : String) {
        jars = listOf(FileSystems.newFileSystem(Paths.get(f), this))
    }
}

//class FlowLoader2(private val archivePath: Path, parent: ClassLoader? = getSystemClassLoader()) : AutoCloseable {
//    private val temporaryDir: Path
////    private val checkpointSerializationContext : CheckpointSerializationContext
//
//    val classLoader: ClassLoader
//
//    companion object {
//
//        //        private val log = contextLogger()
//        private var quasarAgentRunning = false
//
//        private val quasarAgentArgs = """x(antlr**;bftsmart**;co.pa
//            ralleluniverse**;com.codahale**;com.esotericsoftware**;com.fasterxml*
//            *;com.google**;com.ibm**;com.intellij**;com.jcabi**;com.nhaarman**;co
//            m.opengamma**;com.typesafe**;com.zaxxer**;de.javakaffee**;groovy**;gr
//            oovyjarjarantlr**;groovyjarjarasm**;io.atomix**;io.github**;io.netty*
//            *;jdk**;kotlin**;net.corda.djvm**;djvm**;net.bytebuddy**;net.i2p**;or
//            g.apache**;org.bouncycastle**;org.codehaus**;org.crsh**;org.dom4j**;o
//            rg.fusesource**;org.h2**;org.hibernate**;org.jboss**;org.jcp**;org.jo
//            da**;org.objectweb**;org.objenesis**;org.slf4j**;org.w3c**;org.xml**;
//            org.yaml**;reflectasm**;rx**;org.jolokia**;com.lmax**;picocli**;liqui
//            base**;com.github.benmanes**;org.json**;org.postgresql**;nonapi.io.gi
//            thub.classgraph**)l(net.corda.djvm.**;net.corda.core.serialization.in
//            ternal.**)
//        """.replace("\n", "").replace(" ", "")
//
//        private fun startQuasarAgent(archivePath: Path, temporaryDir: Path) {
//            synchronized(this) {
//                if (!quasarAgentRunning) {
//                    val quasarJar = Files.list(temporaryDir.resolve("lib")).asSequence().firstOrNull {
//                        it.fileName.toString().startsWith("quasar-core")
//                    } ?: throw RuntimeException("Quasar jar not found in $archivePath")
//                    val nameOfRunningVM = ManagementFactory.getRuntimeMXBean().name
//                    val pid = nameOfRunningVM.substring(0, nameOfRunningVM.indexOf('@'))
//                    val vm = VirtualMachine.attach(pid)
//                    vm.loadAgent(quasarJar.toString(), quasarAgentArgs)
//                    vm.detach()
//                    quasarAgentRunning = true
//                }
//            }
//        }
//
//        private fun extract(archivePath: Path, destination: Path) {
//            val buffer = ByteArray(0x10000)
//            ZipInputStream(Files.newInputStream(archivePath)).use { zis ->
//                var zipEntry = zis.nextEntry
//                while (zipEntry != null) {
//                    val newFile: Path = destination.resolve(zipEntry.name)
//                    Files.createDirectories(newFile.parent)
//                    Files.newOutputStream(newFile).use { outputStream ->
//                        while (true) {
//                            val read = zis.read(buffer)
//                            if (read < 0) break
//                            outputStream.write(buffer, 0, read)
//                        }
//                    }
//                    zipEntry = zis.nextEntry
//                }
//                zis.closeEntry()
//            }
//        }
//
//        private fun deletePath(path: Path?) {
//            Files.walk(path)
//                    .sorted(Comparator.reverseOrder())
//                    .map { obj: Path -> obj.toFile() }
//                    .forEach { obj: File -> obj.delete() }
//        }
//
//        @JvmStatic
//        fun main(args: Array<String>) {
//            val runtimeMxBean = ManagementFactory.getRuntimeMXBean()
//            val arguments = runtimeMxBean.inputArguments
//            println(arguments)
//
//            val systemClassLoader = getSystemClassLoader()
////            val res = systemClassLoader.getResource("/net/corda/flow/necromancer/Runner.class")
////            val cls = getSystemClassLoader().loadClass(Runner::class.java.name)
////            throw java.lang.RuntimeException("KA-BOOM!!!")
//            val p = Paths.get("/home/r3/checkpoints_debug.zip")
//            FlowLoader2(p).use { flowLoader ->
//                val cl = flowLoader.classLoader.loadClass("org.slf4j.LoggerFactory")
//                val fl = flowLoader.classLoader.loadClass("org.slf4j.helpers.NOPLogger")
//                val fiberFile = flowLoader.fibers().first()
//                val fib = flowLoader.fiber(fiberFile) as Fiber<*>
//                Fiber.unparkDeserialized(fib, DefaultFiberScheduler.getInstance())
//                val cls2 = fib.javaClass
//                println(cls2)
//            }
//        }
//    }
//
//    init {
//        temporaryDir = Files.createTempDirectory("flowLoader")
//        extract(archivePath, temporaryDir)
//        val jars = (
//                Files.list(temporaryDir.resolve("lib")).asSequence() +
//                        Files.list(temporaryDir.resolve("drivers")).asSequence() +
//                        Files.list(temporaryDir.resolve("cordapps")).asSequence()
//                ).filter {
//                    it.fileName.toString().endsWith(".jar")
//                }.map {
//                    FileSystems.newFileSystem(it, null)
//                }.toList()
////        classLoader = getSystemClassLoader()
//        classLoader = object : ClassLoader(parent) {
//
//            override fun loadClass(name: String, resolve: Boolean): Class<*>? {
//                synchronized(getClassLoadingLock(name)) {
//                    var c = findLoadedClass(name)
//                    if (c == null) {
//                        if (name.startsWith("co.paralleluniverse")) {
//                            //We have to return the same quasar classes used by the running java agent
//                            c = Class.forName(name)
//                        } else {
//                            c = findClass(name)
//                            if (c == null) {
//                                c = super.loadClass(name, resolve)
//                            }
//                        }
//                    }
//                    if (c == null) throw ClassNotFoundException(name)
//                    else if (resolve) {
//                        resolveClass(c)
//                    }
//                    return c
//                }
//            }
//
//            private fun readAllBytes(inputStream: InputStream): ByteArray {
//                val result = ByteArrayOutputStream()
//                val buffer = ByteArray(0x10000)
//                while (true) {
//                    val read = inputStream.read(buffer, 0, buffer.size)
//                    if (read < 0) break
//                    result.write(buffer, 0, read)
//                }
//                return result.toByteArray()
//            }
//
//            override fun findClass(name: String): Class<*>? {
//                val resourceName = name.replace('.', '/') + ".class"
//                return when {
//                    name.startsWith(Runner::class.java.name) ->
//                        getSystemClassLoader().getResourceAsStream(resourceName)
//                    else -> findResource(resourceName)?.let(URL::openStream)
//                }?.use { inputStream ->
//                    val bytes = readAllBytes(inputStream)
//                    defineClass(name, bytes, 0, bytes.size)
//                }
//            }
//
//            override fun findResource(name: String): URL? = findResources(name).let {
//                if (it.hasMoreElements()) it.nextElement() else null
//            }
//
//            override fun getResource(name: String): URL? {
//                val resourceEnumeration = getResources(name)
//                return if(resourceEnumeration.hasMoreElements()) resourceEnumeration.nextElement() else null
//            }
//
//            override fun findResources(name: String): Enumeration<URL> {
//                val it = jars.asSequence().map {
//                    it.getPath(name)
//                }.filter {
//                    Files.exists(it)
//                }.map {
//                    it.toUri().toURL()
//                }.iterator()
//                return object : Enumeration<URL> {
//                    override fun hasMoreElements(): Boolean = it.hasNext()
//                    override fun nextElement(): URL = it.next()
//                }
//            }
//
//            override fun getResources(name: String): Enumeration<URL> {
//                return CompoundEnumeration(arrayOf(findResources(name), parent?.getResources(name)))
//            }
//        }
////        Thread.currentThread().contextClassLoader = classLoader
//
////        nodeSerializationEnv = SerializationEnvironment.with(
////                SerializationFactoryImpl().apply
////                {
////                    registerScheme(AMQPServerSerializationScheme(
////                            emptyList()))
////                    registerScheme(AMQPClientSerializationScheme(
////                            emptyList()))
////                },
////                p2pContext = AMQP_P2P_CONTEXT.withClassLoader(classLoader = classLoader),
////                rpcServerContext = AMQP_RPC_SERVER_CONTEXT.withClassLoader(classLoader),
////                rpcClientContext = AMQP_RPC_CLIENT_CONTEXT.withClassLoader(classLoader),
////                storageContext = AMQP_STORAGE_CONTEXT.withClassLoader(classLoader),
////                checkpointSerializer = KryoCheckpointSerializer,
////                checkpointContext = KRYO_CHECKPOINT_CONTEXT.withClassLoader(classLoader)
////        )
////
////
////        val nodeServicesContext = newProxyObject<NodeServicesContext> { _: Any?, _: Method, _: Array<Any?>? ->
////            TODO("Not yet implemented")
////        }
////
////        val serviceHub = newProxyObject<ServiceHub> { _: Any?, _: Method, _: Array<Any?>? ->
////            TODO("Not yet implemented")
////        }
////
////        val res = classLoader.loadClass("co.paralleluniverse.fibers.instrument.JavaAgent").getMethod("isActive").invoke(null) as Boolean
////
////        check(res) {
////            "Missing the '-javaagent' JVM argument. Make sure you run the tests with the Quasar java agent attached to your JVM."
////        }
////
////        checkpointSerializationContext = CheckpointSerializationDefaults.CHECKPOINT_CONTEXT.withTokenContext(
////                CheckpointSerializeAsTokenContextImpl(
////                        nodeServicesContext,
////                        CheckpointSerializationDefaults.CHECKPOINT_SERIALIZER,
////                        CheckpointSerializationDefaults.CHECKPOINT_CONTEXT,
////                        serviceHub
////                )
////        )
//    }
//
//    fun fibers(): Sequence<String> = Files.list(temporaryDir.resolve("fibers")).asSequence()
//            .map { it.fileName.toString() }
//            .filter { it.endsWith(".fiber") }
//
//    fun fiber(name: String): Any {
//        val runnerClass = classLoader.loadClass("net.corda.flow.necromancer.Runner")
//        val constructor = runnerClass.getConstructor(ClassLoader::class.java, Path::class.java, Path::class.java)
//        val runMethod = runnerClass.getMethod("run", Path::class.java)
//        val runner = constructor.newInstance(classLoader, archivePath, temporaryDir)
//        return runMethod.invoke(runner, temporaryDir.resolve("fibers/$name"))
//    }
//
//    private fun deletePath(path: Path?) {
//        Files.walk(path)
//                .sorted(Comparator.reverseOrder())
//                .map { obj: Path -> obj.toFile() }
//                .forEach { obj: File -> obj.delete() }
//    }
//
//    override fun close() {
//        deletePath(temporaryDir)
//    }
//}
//
//class Runner(classLoader: ClassLoader, archivePath: Path, temporaryDir: Path) {
//
//    private inline fun <reified T> newProxyObject(crossinline methodInvocation: (proxy: Any?, method: Method, args: Array<Any?>?) -> Any) =
//            Proxy.newProxyInstance(T::class.javaClass.classLoader, arrayOf<Class<*>>(T::class.java)) { proxy, method, args ->
//                methodInvocation(proxy, method, args)
//            } as T
//
//    private val checkpointSerializationContext: CheckpointSerializationContext
//
//    private var quasarAgentRunning = false
//
//    private val quasarAgentArgs = """x(antlr**;bftsmart**;co.pa
//            ralleluniverse**;com.codahale**;com.esotericsoftware**;com.fasterxml*
//            *;com.google**;com.ibm**;com.intellij**;com.jcabi**;com.nhaarman**;co
//            m.opengamma**;com.typesafe**;com.zaxxer**;de.javakaffee**;groovy**;gr
//            oovyjarjarantlr**;groovyjarjarasm**;io.atomix**;io.github**;io.netty*
//            *;jdk**;kotlin**;net.corda.djvm**;djvm**;net.bytebuddy**;net.i2p**;or
//            g.apache**;org.bouncycastle**;org.codehaus**;org.crsh**;org.dom4j**;o
//            rg.fusesource**;org.h2**;org.hibernate**;org.jboss**;org.jcp**;org.jo
//            da**;org.objectweb**;org.objenesis**;org.slf4j**;org.w3c**;org.xml**;
//            org.yaml**;reflectasm**;rx**;org.jolokia**;com.lmax**;picocli**;liqui
//            base**;com.github.benmanes**;org.json**;org.postgresql**;nonapi.io.gi
//            thub.classgraph**)l(net.corda.djvm.**;net.corda.core.serialization.in
//            ternal.**)
//        """.replace("\n", "").replace(" ", "")
//
//    private fun startQuasarAgent(archivePath: Path, temporaryDir: Path) {
//        synchronized(this) {
//            if (!quasarAgentRunning) {
//                val quasarJar = Files.list(temporaryDir.resolve("lib")).asSequence().firstOrNull {
//                    it.fileName.toString().startsWith("quasar-core")
//                } ?: throw RuntimeException("Quasar jar not found in $archivePath")
//                val nameOfRunningVM = ManagementFactory.getRuntimeMXBean().name
//                val pid = nameOfRunningVM.substring(0, nameOfRunningVM.indexOf('@'))
//                val vm = VirtualMachine.attach(pid)
//                vm.loadAgent(quasarJar.toString(), quasarAgentArgs)
//                vm.detach()
//                quasarAgentRunning = true
//            }
//        }
//    }
//
//    init {
//        startQuasarAgent(archivePath, temporaryDir)
//        Thread.currentThread().contextClassLoader = classLoader
//
//        nodeSerializationEnv = SerializationEnvironment.with(
//                SerializationFactoryImpl().apply
//                {
//                    registerScheme(AMQPServerSerializationScheme(
//                            emptyList()))
//                    registerScheme(AMQPClientSerializationScheme(
//                            emptyList()))
//                },
//                p2pContext = AMQP_P2P_CONTEXT.withClassLoader(classLoader = classLoader),
//                rpcServerContext = AMQP_RPC_SERVER_CONTEXT.withClassLoader(classLoader),
//                rpcClientContext = AMQP_RPC_CLIENT_CONTEXT.withClassLoader(classLoader),
//                storageContext = AMQP_STORAGE_CONTEXT.withClassLoader(classLoader),
//                checkpointSerializer = KryoCheckpointSerializer,
//                checkpointContext = KRYO_CHECKPOINT_CONTEXT.withClassLoader(classLoader)
//        )
//
//        val nodeServicesContext = newProxyObject<NodeServicesContext> { _: Any?, _: Method, _: Array<Any?>? ->
//            TODO("Not yet implemented")
//        }
//
//        val serviceHub = newProxyObject<ServiceHub> { _: Any?, _: Method, _: Array<Any?>? ->
//            TODO("Not yet implemented")
//        }
//
//        val res = classLoader.loadClass("co.paralleluniverse.fibers.instrument.JavaAgent").getMethod("isActive").invoke(null) as Boolean
//
//        check(res) {
//            "Missing the '-javaagent' JVM argument. Make sure you run the tests with the Quasar java agent attached to your JVM."
//        }
//
//        checkpointSerializationContext = CheckpointSerializationDefaults.CHECKPOINT_CONTEXT.withTokenContext(
//                CheckpointSerializeAsTokenContextImpl(
//                        nodeServicesContext,
//                        CheckpointSerializationDefaults.CHECKPOINT_SERIALIZER,
//                        CheckpointSerializationDefaults.CHECKPOINT_CONTEXT,
//                        serviceHub
//                )
//        )
//    }
//
//    fun run(fiberFile: Path) {
//        val fiber = Files.readAllBytes(fiberFile).checkpointDeserialize(checkpointSerializationContext) as Fiber<*>
//        Fiber.unparkDeserialized(fiber, DefaultFiberScheduler.getInstance())
//    }
//}