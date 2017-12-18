package net.corda.node.services.transactions

import com.codahale.metrics.MetricRegistry
import com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import net.corda.core.contracts.StateRef
import net.corda.core.crypto.SecureHash
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.node.services.UniquenessException
import net.corda.core.node.services.UniquenessProvider
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.loggerFor
import java.security.PublicKey
import java.sql.BatchUpdateException
import java.sql.Connection
import java.util.*

/**
 * Uniqueness provider backed by a MySQL database. It is intended to be used with a multi-master synchronously replicated
 * variant of MySQL, such as Percona XtraDB Cluster, or MariaDB Galera Cluster.
 *
 * Note that no ORM is used since we want to retain full control over table schema and be able to experiment with optimisations.
 */
class MySQLUniquenessProvider(
        metrics: MetricRegistry,
        dataSourceProperties: Properties
) : UniquenessProvider, SingletonSerializeAsToken() {
    companion object {
        private val log = loggerFor<MySQLUniquenessProvider>()

        // TODO: optimize table schema for InnoDB
        private val createTableStatement =
                "CREATE TABLE IF NOT EXISTS committed_states (" +
                        "issue_tx_id BINARY(32) NOT NULL," +
                        "issue_tx_output_id INT NOT NULL," +
                        "consuming_tx_id BINARY(32) NOT NULL," +
                        "consuming_tx_input_id INT UNSIGNED NOT NULL," +
                        "consuming_party_name TEXT NOT NULL," +
                        // TODO: do we need to store the key? X500 name should be sufficient
                        "consuming_party_key BLOB NOT NULL," +
                        "commit_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                        "CONSTRAINT id PRIMARY KEY (issue_tx_id, issue_tx_output_id)" +
                        ")"
        private val insertStatement = "INSERT INTO committed_states (issue_tx_id, issue_tx_output_id, consuming_tx_id, consuming_tx_input_id, consuming_party_name, consuming_party_key) VALUES (?, ?, ?, ?, ?, ?)"
        private val findStatement = "SELECT consuming_tx_id, consuming_tx_input_id, consuming_party_name, consuming_party_key FROM committed_states WHERE issue_tx_id = ? AND issue_tx_output_id = ?"
    }

    private val metricPrefix = MySQLUniquenessProvider::class.simpleName
    /** Transaction commit duration and rate metric timer */
    private val commitTimer = metrics.timer("$metricPrefix.Commit")
    /**
     * When writing to multiple masters with Galera, transaction rollbacks may happen due to high write contention.
     * This is a useful heath metric.
     */
    private val rollbackCounter = metrics.counter("$metricPrefix.Rollback")
    /** Track double spend attempts. Note that this will also include notarisation retries. */
    private val conflictCounter = metrics.counter("$metricPrefix.Conflicts")

    val dataSource = HikariDataSource(HikariConfig(dataSourceProperties))

    private val connection: Connection
        get() = dataSource.connection

    fun createTable() {
        log.debug("Attempting to create DB table if it does not yet exist: $createTableStatement")
        connection.use {
            it.createStatement().execute(createTableStatement)
            it.commit()
        }
    }

    fun stop() {
        dataSource.close()
    }

    override fun commit(states: List<StateRef>, txId: SecureHash, callerIdentity: Party) {
        val timer = commitTimer.time()
        try {
            retryTransaction(CommitAll(states, txId, callerIdentity))
        } catch (e: BatchUpdateException) {
            log.info("Unable to commit input states, finding conflicts", e)
            conflictCounter.inc()
            retryTransaction(FindConflicts(states))
        } finally {
            timer.stop()
        }
    }

    private fun retryTransaction(tx: RetryableTransaction) {
        connection.use {
            while (true) {
                try {
                    tx.run(it)
                } catch (e: Exception) {
                    it.rollback()
                    if (e is MySQLTransactionRollbackException) {
                        log.warn("Rollback exception occurred, retrying", e)
                        rollbackCounter.inc()
                        continue
                    } else {
                        throw e
                    }
                }
                break
            }
            it.commit()
        }
    }

    interface RetryableTransaction {
        fun run(conn: Connection)
    }

    private class CommitAll(val states: List<StateRef>, val txId: SecureHash, val callerIdentity: Party) : RetryableTransaction {
        override fun run(conn: Connection) {
            conn.prepareStatement(insertStatement).apply {
                states.forEachIndexed { index, stateRef ->
                    // StateRef
                    setBytes(1, stateRef.txhash.bytes)
                    setInt(2, stateRef.index)
                    // Consuming transaction
                    setBytes(3, txId.bytes)
                    setInt(4, index)
                    setString(5, callerIdentity.name.toString())
                    setBytes(6, callerIdentity.owningKey.serialize().bytes)

                    addBatch()
                    clearParameters()
                }
                executeBatch()
                close()
            }
        }
    }

    private class FindConflicts(val states: List<StateRef>) : RetryableTransaction {
        override fun run(conn: Connection) {
            val conflicts = mutableMapOf<StateRef, UniquenessProvider.ConsumingTx>()
            states.forEach {
                val st = conn.prepareStatement(findStatement).apply {
                    setBytes(1, it.txhash.bytes)
                    setInt(2, it.index)
                }
                val result = st.executeQuery()

                if (result.next()) {
                    val consumingTxId = SecureHash.SHA256(result.getBytes(1))
                    val inputIndex = result.getInt(2)
                    val partyName = CordaX500Name.parse(result.getString(3))
                    val partyKey: PublicKey = result.getBytes(4).deserialize()
                    conflicts[it] = UniquenessProvider.ConsumingTx(consumingTxId, inputIndex, Party(partyName, partyKey))
                }
            }
            conn.commit()
            if (conflicts.isNotEmpty()) throw UniquenessException(UniquenessProvider.Conflict(conflicts))
        }
    }
}