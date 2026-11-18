/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlinx.coroutines.runBlocking
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.mock
import org.opensearch.Version
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.mockito.ArgumentMatchers.anyBoolean
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.`when`
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.alerting.MonitorRunnerService
import org.opensearch.alerting.parser
import org.opensearch.alerting.randomDocumentLevelMonitor
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.AliasMetadata
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.routing.RoutingTable
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.client.AdminClient
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.IndicesAdminClient
import kotlin.test.assertFailsWith
import org.mockito.Mockito.`when` as whenever

import java.util.concurrent.atomic.AtomicInteger

/**
 * Unit tests for [DocLevelMonitorQueries].
 *
 * Covers:
 *  - [DocLevelMonitorQueries.deepCopyMap] — deep isolation of nested maps and lists of maps.
 *  - [DocLevelMonitorQueries.findConflictingFields] — cross-index type conflict detection,
 *    alias target path divergence detection across rollover boundaries, and transitive
 *    propagation of target field conflicts to alias fields.
 *  - [DocLevelMonitorQueries.buildLeafNodeProcessor] — per-index vs shared suffixing for
 *    both regular fields and alias fields + their `path` targets, including nested aliases.
 *  - [DocLevelMonitorQueries.indexDocLevelQueries] — bounded single-retry guard when
 *    `deleteQueryIndexInEveryRun` is enabled and `putMapping` persistently fails.
 */
class DocLevelMonitorQueriesTests : OpenSearchTestCase() {

    private lateinit var client: Client
    private lateinit var adminClient: AdminClient
    private lateinit var indicesAdminClient: IndicesAdminClient
    private lateinit var clusterService: ClusterService

    override fun setUp() {
        super.setUp()
        client = mock(Client::class.java)
        adminClient = mock(AdminClient::class.java)
        indicesAdminClient = mock(IndicesAdminClient::class.java)
        clusterService = mock(ClusterService::class.java)

        whenever(client.admin()).thenReturn(adminClient)
        whenever(adminClient.indices()).thenReturn(indicesAdminClient)

        Mockito.doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            listener.onResponse(AcknowledgedResponse(true))
            null
        }.`when`(indicesAdminClient).putMapping(any(PutMappingRequest::class.java), any())
    }

    private class TestableDocLevelMonitorQueries(
        client: Client,
        clusterService: ClusterService,
        var resolvedIndexName: String? = "concrete-query-index-000001",
        var rolledOverIndexName: String = "concrete-query-index-000002"
    ) : DocLevelMonitorQueries(client, clusterService) {
        var getWriteIndexNameForAliasCallCount = 0
        var lastAliasQueried: String? = null
        var lastPutConcreteQueryIndex: String? = null

        override fun getWriteIndexNameForAlias(alias: String): String? {
            getWriteIndexNameForAliasCallCount++
            lastAliasQueried = alias
            return resolvedIndexName
        }

        override suspend fun adjustMaxFieldLimitForQueryIndex(sourceIndexFieldLimit: Long, concreteQueryIndex: String) {
            lastPutConcreteQueryIndex = concreteQueryIndex
        }

        // Override rolloverQueryIndex to avoid calling monitorCtx (null in unit tests).
        // Returns a configurable new index name for test isolation.
        override suspend fun rolloverQueryIndex(monitor: Monitor): String {
            return rolledOverIndexName
        }
    }

    private fun createMonitor(
        queryIndexAlias: String = "query-index-alias",
        deleteQueryIndexInEveryRun: Boolean? = true,
        monitorId: String = "test-monitor-id"
    ): Monitor {
        val dataSources = DataSources(queryIndex = queryIndexAlias, queryIndexMappingsByType = emptyMap())
        return randomDocumentLevelMonitor(dataSources = dataSources).copy(
            id = monitorId,
            deleteQueryIndexInEveryRun = deleteQueryIndexInEveryRun
        )
    }

    private fun createMonitorMetadata(
        monitorId: String = "test-monitor-id",
        sourceIndex: String = "test-source-index",
        initialTargetQueryIndex: String? = null
    ): MonitorMetadata {
        val mapping = mutableMapOf<String, String>()
        if (initialTargetQueryIndex != null) {
            mapping[sourceIndex + monitorId] = initialTargetQueryIndex
        }
        return MonitorMetadata(
            id = "$monitorId-metadata",
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            monitorId = monitorId,
            lastActionExecutionTimes = emptyList(),
            lastRunContext = emptyMap(),
            sourceToQueryIndexMapping = mapping
        )
    }

    fun `test getWriteIndexNameForAlias returns write index name when alias has write index`() {
        val aliasName = "test-query-alias"
        val concreteIndex = "concrete-index-000001"

        val indexMetadata = IndexMetadata.builder(concreteIndex)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(aliasName).writeIndex(true).build())
            .build()
        val metadata = Metadata.builder().put(indexMetadata, false).build()
        val clusterState = ClusterState.builder(ClusterName("test-cluster")).metadata(metadata).build()
        whenever(clusterService.state()).thenReturn(clusterState)

        val docLevelMonitorQueries = DocLevelMonitorQueries(client, clusterService)
        val writeIndex = docLevelMonitorQueries.getWriteIndexNameForAlias(aliasName)

        assertEquals("Expected write index name to match concrete backing index", concreteIndex, writeIndex)
    }

    fun `test getWriteIndexNameForAlias returns null when alias does not exist`() {
        val metadata = Metadata.builder().build()
        val clusterState = ClusterState.builder(ClusterName("test-cluster")).metadata(metadata).build()
        whenever(clusterService.state()).thenReturn(clusterState)

        val docLevelMonitorQueries = DocLevelMonitorQueries(client, clusterService)
        val writeIndex = docLevelMonitorQueries.getWriteIndexNameForAlias("non-existent-alias")

        assertNull("Expected null write index for non-existent alias", writeIndex)
    }

    fun `test updateQueryIndexMappings when targetQueryIndex is concrete and deleteQueryIndexInEveryRun true skips resolution`() {
        val monitor = createMonitor(queryIndexAlias = "query-index-alias", deleteQueryIndexInEveryRun = true)
        val sourceIndex = "test-source-index"
        val existingConcreteIndex = "concrete-query-index-000001"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = existingConcreteIndex
        )

        val testableQueries = TestableDocLevelMonitorQueries(client, clusterService)

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to be the existing concrete index", existingConcreteIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should NOT be called when concrete index already exists",
            0,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals(
            "Metadata mapping should preserve existing concrete index",
            existingConcreteIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the concrete index, not the alias",
            existingConcreteIndex,
            testableQueries.lastPutConcreteQueryIndex
        )
    }

    fun `test updateQueryIndexMappings when deleteQueryIndexInEveryRun true and targetQueryIndex equals alias resolves concrete index`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = true)
        val sourceIndex = "test-source-index"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = aliasName
        )

        val concreteResolvedIndex = "concrete-query-index-000002"
        val testableQueries = TestableDocLevelMonitorQueries(
            client,
            clusterService,
            resolvedIndexName = concreteResolvedIndex
        )

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to be the newly resolved concrete index", concreteResolvedIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should be called once to fix alias-as-target",
            1,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals("Alias queried should match monitor queryIndex alias", aliasName, testableQueries.lastAliasQueried)
        assertEquals(
            "Metadata mapping should be updated to resolved concrete index",
            concreteResolvedIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the resolved concrete index, not the alias",
            concreteResolvedIndex,
            testableQueries.lastPutConcreteQueryIndex
        )
    }

    fun `test updateQueryIndexMappings when targetQueryIndex is null resolves concrete index`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = true)
        val sourceIndex = "test-source-index"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = null
        )

        val concreteResolvedIndex = "concrete-query-index-000003"
        val testableQueries = TestableDocLevelMonitorQueries(
            client,
            clusterService,
            resolvedIndexName = concreteResolvedIndex
        )

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to be the newly resolved concrete index", concreteResolvedIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should be called once when targetQueryIndex is null",
            1,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals("Alias queried should match monitor queryIndex alias", aliasName, testableQueries.lastAliasQueried)
        assertEquals(
            "Metadata mapping should be set to resolved concrete index",
            concreteResolvedIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the resolved concrete index, not the alias",
            concreteResolvedIndex,
            testableQueries.lastPutConcreteQueryIndex
        )
    }

    fun `test updateQueryIndexMappings when deleteQueryIndexInEveryRun false retains concrete index without resolution`() {
        val monitor = createMonitor(queryIndexAlias = "query-index-alias", deleteQueryIndexInEveryRun = false)
        val sourceIndex = "test-source-index"
        val existingConcreteIndex = "concrete-query-index-000001"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = existingConcreteIndex
        )

        val testableQueries = TestableDocLevelMonitorQueries(client, clusterService)

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to be the existing concrete index", existingConcreteIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should NOT be called when deleteQueryIndexInEveryRun is false",
            0,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals(
            "Metadata mapping should preserve existing concrete index",
            existingConcreteIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the concrete index, not the alias",
            existingConcreteIndex,
            testableQueries.lastPutConcreteQueryIndex
        )
    }

    fun `test updateQueryIndexMappings when deleteQueryIndexInEveryRun null retains concrete index without resolution`() {
        val monitor = createMonitor(queryIndexAlias = "query-index-alias", deleteQueryIndexInEveryRun = null)
        val sourceIndex = "test-source-index"
        val existingConcreteIndex = "concrete-query-index-000001"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = existingConcreteIndex
        )

        val testableQueries = TestableDocLevelMonitorQueries(client, clusterService)

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to be the existing concrete index", existingConcreteIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should NOT be called when deleteQueryIndexInEveryRun is null",
            0,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals(
            "Metadata mapping should preserve existing concrete index",
            existingConcreteIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the concrete index, not the alias",
            existingConcreteIndex,
            testableQueries.lastPutConcreteQueryIndex
        )
    }

    fun `test updateQueryIndexMappings when deleteQueryIndexInEveryRun false and targetQueryIndex equals alias does not re-resolve`() {
        // Accepted trade-off: when deleteQueryIndexInEveryRun==false and the alias name is stored in metadata,
        // the stale alias is never corrected in this code path. OpenSearch routes PutMapping against an alias to its
        // write index, so issuing a PutMappingRequest with the alias name is a valid (if non-ideal) operation.
        // The alias-corruption is only fixed when deleteQueryIndexInEveryRun==true triggers re-resolution.
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = false)
        val sourceIndex = "test-source-index"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = aliasName
        )

        val testableQueries = TestableDocLevelMonitorQueries(client, clusterService)

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to remain unchanged", aliasName, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should NOT be called when deleteQueryIndexInEveryRun is false",
            0,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        // The alias name is passed through to adjustMaxFieldLimitForQueryIndex in this accepted trade-off path.
        // OpenSearch routes the resulting UpdateSettings call to the alias's write index, so this is valid.
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex receives alias name in the accepted trade-off path",
            aliasName,
            testableQueries.lastPutConcreteQueryIndex
        )
    }

    fun `test updateQueryIndexMappings throws AlertingException when getWriteIndexNameForAlias returns null`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = true)
        val sourceIndex = "test-source-index"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = null
        )

        val testableQueries = TestableDocLevelMonitorQueries(
            client,
            clusterService,
            resolvedIndexName = null
        )

        val exception = assertFailsWith<AlertingException> {
            runBlocking {
                testableQueries.updateQueryIndexMappings(
                    monitor = monitor,
                    monitorMetadata = metadata,
                    sourceIndex = sourceIndex,
                    sourceIndexFieldLimit = 1000L,
                    updatedProperties = mutableMapOf(),
                    indexTimeout = TimeValue.timeValueSeconds(30)
                )
            }
        }

        assertTrue(
            "Exception message should mention failure to get write index for alias",
            exception.message?.contains("Failed to get write index for queryIndex alias:$aliasName") == true
        )
        assertEquals(
            "getWriteIndexNameForAlias should have been attempted once",
            1,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
    }


    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private fun makeInstance(): DocLevelMonitorQueries =
        DocLevelMonitorQueries(mock(Client::class.java), mock(ClusterService::class.java))

    // -------------------------------------------------------------------------
    // deepCopyMap — structural correctness
    // -------------------------------------------------------------------------

    fun `test deepCopyMap returns an equal but distinct map reference`() {
        val original = mutableMapOf<String, Any>("key" to "value")

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        assertEquals(original, copy)
        assertNotSame(original, copy)
    }

    fun `test deepCopyMap deep copies nested maps so mutations are isolated`() {
        val nested = mutableMapOf<String, Any>("inner" to "original")
        val original = mutableMapOf<String, Any>("outer" to nested)

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        // Mutate the nested map in the copy.
        @Suppress("UNCHECKED_CAST")
        (copy["outer"] as MutableMap<String, Any>)["inner"] = "mutated"

        // Original nested map must be unaffected.
        assertEquals("original", nested["inner"])
    }

    fun `test deepCopyMap copies nested map references independently`() {
        val nested = mutableMapOf<String, Any>("inner" to "value")
        val original = mutableMapOf<String, Any>("outer" to nested)

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        assertNotSame(nested, copy["outer"])
    }

    fun `test deepCopyMap produces a distinct mutable list for list values`() {
        val list = mutableListOf("a", "b", "c")
        val original = mutableMapOf<String, Any>("items" to list)

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        @Suppress("UNCHECKED_CAST")
        val copiedList = copy["items"] as MutableList<String>
        copiedList.add("d")

        // The list in the copy is a distinct mutable list; original list unchanged.
        assertEquals(3, list.size)
        assertEquals(4, copiedList.size)
        assertNotSame(list, copiedList)
    }

    fun `test deepCopyMap preserves scalar values`() {
        val original = mutableMapOf<String, Any>(
            "strVal" to "hello",
            "intVal" to 42,
            "boolVal" to true,
            "longVal" to 9_999_999_999L
        )

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        assertEquals("hello", copy["strVal"])
        assertEquals(42, copy["intVal"])
        assertEquals(true, copy["boolVal"])
        assertEquals(9_999_999_999L, copy["longVal"])
    }

    fun `test deepCopyMap handles empty map`() {
        val copy = DocLevelMonitorQueries.deepCopyMap(emptyMap())

        assertTrue(copy.isEmpty())
    }

    fun `test deepCopyMap handles multiply-nested structure`() {
        val original = mutableMapOf<String, Any>(
            "level1" to mutableMapOf<String, Any>(
                "level2" to mutableMapOf<String, Any>(
                    "level3" to "leaf"
                )
            )
        )

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        @Suppress("UNCHECKED_CAST")
        val l1 = copy["level1"] as MutableMap<String, Any>
        @Suppress("UNCHECKED_CAST")
        val l2 = l1["level2"] as MutableMap<String, Any>
        assertEquals("leaf", l2["level3"])

        // Mutate deep copy — original must be unaffected.
        l2["level3"] = "changed"
        @Suppress("UNCHECKED_CAST")
        val origL2 = (original["level1"] as Map<String, Any>)["level2"] as Map<String, Any>
        assertEquals("leaf", origL2["level3"])
    }

    fun `test deepCopyMap deep copies map elements inside lists`() {
        val innerMap = mutableMapOf<String, Any>("nested_key" to "original_value")
        val original = mutableMapOf<String, Any>(
            "items" to mutableListOf<Any>(innerMap, "plain_string")
        )

        val copy = DocLevelMonitorQueries.deepCopyMap(original)

        @Suppress("UNCHECKED_CAST")
        val copiedList = copy["items"] as MutableList<Any>
        @Suppress("UNCHECKED_CAST")
        val copiedInnerMap = copiedList[0] as MutableMap<String, Any>

        // Mutate the map element inside the copied list.
        copiedInnerMap["nested_key"] = "mutated_value"

        // The original inner map must be unaffected — deep copy isolated it.
        assertEquals(
            "Map element inside list must be deep-copied so mutations do not affect the original",
            "original_value",
            innerMap["nested_key"]
        )
        assertNotSame(innerMap, copiedInnerMap)
    }

    // -------------------------------------------------------------------------
    // findConflictingFields — real conflict detection and alias propagation
    // -------------------------------------------------------------------------

    fun `test findConflictingFields detects text-to-keyword type change as conflict`() {
        val queries = makeInstance()
        val indexA = mapOf<String, Any>(
            "host" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "name" to mapOf<String, Any>("type" to "text")
                )
            ),
            "event_id" to mapOf<String, Any>("type" to "keyword")
        )
        val indexB = mapOf<String, Any>(
            "host" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "name" to mapOf<String, Any>("type" to "keyword")
                )
            ),
            "event_id" to mapOf<String, Any>("type" to "keyword")
        )

        val conflictingFields = queries.findConflictingFields(listOf(indexA, indexB))

        assertTrue(
            "host.name must be flagged as conflicting because its type changed from text to keyword",
            conflictingFields.contains("host.name")
        )
        assertFalse(
            "event_id must not be flagged as conflicting because it is identical in both indices",
            conflictingFields.contains("event_id")
        )
    }

    fun `test findConflictingFields detects alias path change across rollover as conflict`() {
        val queries = makeInstance()
        val indexA = mapOf<String, Any>(
            "ts" to mapOf<String, Any>("type" to "date"),
            "timestamp" to mapOf<String, Any>("type" to "alias", "path" to "ts")
        )
        val indexB = mapOf<String, Any>(
            "@timestamp" to mapOf<String, Any>("type" to "date"),
            "timestamp" to mapOf<String, Any>("type" to "alias", "path" to "@timestamp")
        )

        val conflictingFields = queries.findConflictingFields(listOf(indexA, indexB))

        assertTrue(
            "timestamp alias with differing path targets across rollover must be flagged as conflicting",
            conflictingFields.contains("timestamp")
        )
        assertFalse("ts target field must not be flagged as conflicting", conflictingFields.contains("ts"))
        assertFalse("@timestamp target field must not be flagged as conflicting", conflictingFields.contains("@timestamp"))
    }

    fun `test findConflictingFields propagates target field conflict to alias field`() {
        val queries = makeInstance()
        val indexA = mapOf<String, Any>(
            "resource" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "host" to mapOf<String, Any>(
                        "properties" to mapOf<String, Any>(
                            "name" to mapOf<String, Any>("type" to "text")
                        )
                    )
                )
            ),
            "host" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "name" to mapOf<String, Any>("type" to "alias", "path" to "resource.host.name")
                )
            )
        )
        val indexB = mapOf<String, Any>(
            "resource" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "host" to mapOf<String, Any>(
                        "properties" to mapOf<String, Any>(
                            "name" to mapOf<String, Any>("type" to "keyword")
                        )
                    )
                )
            ),
            "host" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "name" to mapOf<String, Any>("type" to "alias", "path" to "resource.host.name")
                )
            )
        )

        val conflictingFields = queries.findConflictingFields(listOf(indexA, indexB))

        assertTrue(
            "resource.host.name target field must be flagged as conflicting",
            conflictingFields.contains("resource.host.name")
        )
        assertTrue(
            "host.name alias must inherit conflict from its conflicting target resource.host.name",
            conflictingFields.contains("host.name")
        )
    }

    fun `test findConflictingFields does not flag identical alias or concrete fields`() {
        val queries = makeInstance()
        val indexA = mapOf<String, Any>(
            "message" to mapOf<String, Any>("type" to "text"),
            "event" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "start" to mapOf<String, Any>("type" to "date")
                )
            ),
            "timestamp" to mapOf<String, Any>("type" to "alias", "path" to "event.start")
        )
        val indexB = mapOf<String, Any>(
            "message" to mapOf<String, Any>("type" to "text"),
            "event" to mapOf<String, Any>(
                "properties" to mapOf<String, Any>(
                    "start" to mapOf<String, Any>("type" to "date")
                )
            ),
            "timestamp" to mapOf<String, Any>("type" to "alias", "path" to "event.start")
        )

        val conflictingFields = queries.findConflictingFields(listOf(indexA, indexB))

        assertTrue("Identical concrete and alias mappings must produce no conflicts", conflictingFields.isEmpty())
    }

    // -------------------------------------------------------------------------
    // buildLeafNodeProcessor — alias and conflict suffixing via real traversal
    // -------------------------------------------------------------------------

    fun `test buildLeafNodeProcessor suffixes alias path with updatedIndexName when target is not conflicting`() {
        val queries = makeInstance()
        val properties = mutableMapOf<String, Any>(
            "@timestamp" to mutableMapOf<String, Any>("type" to "date"),
            "timestamp" to mutableMapOf<String, Any>("type" to "alias", "path" to "@timestamp")
        )
        val processor = queries.buildLeafNodeProcessor(
            monitorId = "mon1",
            concreteIndexName = "logs-000002",
            updatedIndexName = "logs",
            conflictingFields = setOf("timestamp")
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(properties, "", processor, flattenPaths)

        assertFalse("Original alias key must be renamed", properties.containsKey("timestamp"))
        assertTrue("Conflicting alias must be suffixed with concreteIndexName", properties.containsKey("timestamp_logs-000002_mon1"))
        val aliasProps = properties["timestamp_logs-000002_mon1"] as Map<*, *>
        assertEquals("alias", aliasProps["type"])
        assertEquals("@timestamp_logs_mon1", aliasProps["path"])
        assertTrue("Non-conflicting target must be suffixed with updatedIndexName", properties.containsKey("@timestamp_logs_mon1"))
    }

    fun `test buildLeafNodeProcessor suffixes both alias and path with concreteIndexName when target is conflicting`() {
        val queries = makeInstance()
        val properties = mutableMapOf<String, Any>(
            "resource" to mutableMapOf<String, Any>(
                "properties" to mutableMapOf<String, Any>(
                    "host" to mutableMapOf<String, Any>(
                        "properties" to mutableMapOf<String, Any>(
                            "name" to mutableMapOf<String, Any>("type" to "keyword")
                        )
                    )
                )
            ),
            "host" to mutableMapOf<String, Any>(
                "properties" to mutableMapOf<String, Any>(
                    "name" to mutableMapOf<String, Any>("type" to "alias", "path" to "resource.host.name")
                )
            )
        )
        val processor = queries.buildLeafNodeProcessor(
            monitorId = "mon1",
            concreteIndexName = "logs-000002",
            updatedIndexName = "logs",
            conflictingFields = setOf("resource.host.name", "host.name")
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(properties, "", processor, flattenPaths)

        val hostProps = (properties["host"] as Map<*, *>)["properties"] as Map<*, *>
        assertFalse("Original nested alias leaf must be renamed", hostProps.containsKey("name"))
        assertTrue("Conflicting alias leaf must use concreteIndexName suffix", hostProps.containsKey("name_logs-000002_mon1"))
        val aliasProps = hostProps["name_logs-000002_mon1"] as Map<*, *>
        assertEquals("alias", aliasProps["type"])
        assertEquals("resource.host.name_logs-000002_mon1", aliasProps["path"])

        val resourceProps = (properties["resource"] as Map<*, *>)["properties"] as Map<*, *>
        val resourceHostProps = (resourceProps["host"] as Map<*, *>)["properties"] as Map<*, *>
        assertTrue(
            "Conflicting target leaf must use concreteIndexName suffix",
            resourceHostProps.containsKey("name_logs-000002_mon1")
        )
    }

    fun `test buildLeafNodeProcessor preserves nested alias fields with valid suffixed path`() {
        val queries = makeInstance()
        val properties = mutableMapOf<String, Any>(
            "alias" to mutableMapOf<String, Any>(
                "properties" to mutableMapOf<String, Any>(
                    "some" to mutableMapOf<String, Any>(
                        "properties" to mutableMapOf<String, Any>(
                            "fff" to mutableMapOf<String, Any>("type" to "alias", "path" to "test_field.some_other_field")
                        )
                    )
                )
            ),
            "test_field" to mutableMapOf<String, Any>(
                "properties" to mutableMapOf<String, Any>(
                    "some_other_field" to mutableMapOf<String, Any>("type" to "keyword")
                )
            )
        )
        val processor = queries.buildLeafNodeProcessor(
            monitorId = "mon1",
            concreteIndexName = "logs-000001",
            updatedIndexName = "logs",
            conflictingFields = emptySet()
        )
        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()

        queries.traverseMappingsAndUpdate(properties, "", processor, flattenPaths)

        assertTrue(flattenPaths.containsKey("alias.some.fff"))
        assertTrue(flattenPaths.containsKey("test_field.some_other_field"))

        val aliasPropsParent = (properties["alias"] as Map<*, *>)["properties"] as Map<*, *>
        val someProps = (aliasPropsParent["some"] as Map<*, *>)["properties"] as Map<*, *>
        assertTrue("Nested alias leaf must be renamed with monitor suffix", someProps.containsKey("fff_logs_mon1"))
        val aliasProps = someProps["fff_logs_mon1"] as Map<*, *>
        assertFalse("Nested alias mapping must not be empty", aliasProps.isEmpty())
        assertEquals("alias", aliasProps["type"])
        assertEquals("test_field.some_other_field_logs_mon1", aliasProps["path"])

        val testFieldProps = (properties["test_field"] as Map<*, *>)["properties"] as Map<*, *>
        assertTrue("Target field must exist with matching suffixed name", testFieldProps.containsKey("some_other_field_logs_mon1"))
    }

    // -------------------------------------------------------------------------
    // indexDocLevelQueries / updateQueryIndexMappings — bounded retry guard
    // -------------------------------------------------------------------------

    fun `test indexDocLevelQueries bounds delete-and-recreate retry to once on persistent putMapping failure`() {
        val queryIndexAlias = "custom-query-index"
        val sourceIndexContent = "{\"logs-000001\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
            "\"number_of_shards\":\"1\",\"number_of_replicas\":\"0\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
            "\"version\":{\"created\":\"${Version.CURRENT.id}\"},\"provided_name\":\"logs-000001\"}}," +
            "\"mapping_version\":1,\"settings_version\":1,\"aliases_version\":1," +
            "\"mappings\":{\"_doc\":{\"properties\":{\"message\":{\"type\":\"text\"}}}}}}"
        val sourceIndexMetadata = IndexMetadata.fromXContent(parser(sourceIndexContent))

        fun buildQueryIndexMetadata(indexName: String): IndexMetadata = IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(queryIndexAlias).writeIndex(true))
            .build()

        val routingTable = mock(RoutingTable::class.java)
        `when`(routingTable.hasIndex("logs-000001")).thenReturn(true)

        fun stateWithQueryIndex(queryIndexName: String?): ClusterState {
            val metadataBuilder = Metadata.builder().put(sourceIndexMetadata, false)
            if (queryIndexName != null) {
                metadataBuilder.put(buildQueryIndexMetadata(queryIndexName), false)
            }
            return ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadataBuilder.build())
                .routingTable(routingTable)
                .build()
        }

        var currentState = stateWithQueryIndex("$queryIndexAlias-000001")
        val clusterService = mock(ClusterService::class.java)
        `when`(clusterService.state()).thenAnswer { currentState }

        val resolver = mock(IndexNameExpressionResolver::class.java)
        `when`(resolver.concreteIndexNames(any(), any(), anyBoolean(), anyString()))
            .thenReturn(arrayOf("logs-000001"))

        val indicesAdmin = mock(IndicesAdminClient::class.java)
        val adminClient = mock(AdminClient::class.java)
        `when`(adminClient.indices()).thenReturn(indicesAdmin)
        val client = mock(Client::class.java)
        `when`(client.admin()).thenReturn(adminClient)

        val getSettingsResponse = mock(GetSettingsResponse::class.java)
        doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<GetSettingsResponse>
            listener.onResponse(getSettingsResponse)
            null
        }.`when`(indicesAdmin).getSettings(any(GetSettingsRequest::class.java), any())

        doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            listener.onResponse(AcknowledgedResponse(true))
            null
        }.`when`(indicesAdmin).updateSettings(any(UpdateSettingsRequest::class.java), any())

        val putMappingCalls = AtomicInteger(0)
        doAnswer { invocation ->
            putMappingCalls.incrementAndGet()
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            listener.onFailure(IllegalArgumentException("persistent mapper conflict"))
            null
        }.`when`(indicesAdmin).putMapping(any(PutMappingRequest::class.java), any())

        val deleteCalls = AtomicInteger(0)
        doAnswer { invocation ->
            deleteCalls.incrementAndGet()
            currentState = stateWithQueryIndex(null)
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            listener.onResponse(AcknowledgedResponse(true))
            null
        }.`when`(indicesAdmin).delete(any(DeleteIndexRequest::class.java), any())

        val createCalls = AtomicInteger(0)
        val createIndexResponse = mock(CreateIndexResponse::class.java)
        `when`(createIndexResponse.isAcknowledged).thenReturn(true)
        doAnswer { invocation ->
            createCalls.incrementAndGet()
            currentState = stateWithQueryIndex("$queryIndexAlias-000002")
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<CreateIndexResponse>
            listener.onResponse(createIndexResponse)
            null
        }.`when`(indicesAdmin).create(any(CreateIndexRequest::class.java), any())

        val queries = DocLevelMonitorQueries(client, clusterService)
        val monitor = randomDocumentLevelMonitor(
            inputs = listOf(
                DocLevelMonitorInput(
                    description = "desc",
                    indices = listOf("logs*"),
                    queries = listOf(
                        DocLevelQuery(
                            id = "q1",
                            query = "message:error",
                            name = "q1",
                            fields = emptyList(),
                            tags = emptyList()
                        )
                    )
                )
            ),
            triggers = emptyList(),
            dataSources = DataSources(queryIndex = queryIndexAlias)
        ).copy(id = "mon1", deleteQueryIndexInEveryRun = true)
        val monitorMetadata = MonitorMetadata(
            id = "mon1-metadata",
            seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            monitorId = monitor.id,
            lastActionExecutionTimes = emptyList(),
            lastRunContext = emptyMap(),
            sourceToQueryIndexMapping = mutableMapOf()
        )

        val originalMonitorCtx = MonitorRunnerService.monitorCtx
        try {
            MonitorRunnerService.monitorCtx = originalMonitorCtx.copy(
                clusterService = clusterService,
                indexNameExpressionResolver = resolver,
                docLevelMonitorQueries = queries
            )

            val thrown = expectThrows(AlertingException::class.java) {
                runBlocking {
                    queries.indexDocLevelQueries(
                        monitor = monitor,
                        monitorId = monitor.id,
                        monitorMetadata = monitorMetadata,
                        indexTimeout = TimeValue.timeValueSeconds(30)
                    )
                }
            }
            assertTrue(
                "Exception message should propagate underlying putMapping error",
                thrown.message?.contains("persistent mapper conflict") == true
            )
            assertEquals("putMapping should be attempted exactly twice (initial + 1 retry)", 2, putMappingCalls.get())
            assertEquals("Query index should be deleted at most once", 1, deleteCalls.get())
            assertEquals("Query index should be recreated at most once", 1, createCalls.get())
        } finally {
            MonitorRunnerService.monitorCtx = originalMonitorCtx
        }
    }

    // The null branch fires independently of deleteQueryIndexInEveryRun.
    // This is distinct from tests that have a non-null concrete index stored with flag=true/false.
    fun `test updateQueryIndexMappings when targetQueryIndex is null and deleteQueryIndexInEveryRun false still resolves concrete index`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = false)
        val sourceIndex = "test-source-index"
        // No entry in metadata — targetQueryIndex will be null on first run regardless of deleteQueryIndexInEveryRun
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = null
        )

        val concreteResolvedIndex = "concrete-query-index-000001"
        val testableQueries = TestableDocLevelMonitorQueries(
            client,
            clusterService,
            resolvedIndexName = concreteResolvedIndex
        )

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged", response.isAcknowledged)
        assertEquals("Expected target index to be the resolved concrete index", concreteResolvedIndex, targetIndex)
        assertEquals(
            "getWriteIndexNameForAlias should be called once when targetQueryIndex is null, regardless of deleteQueryIndexInEveryRun",
            1,
            testableQueries.getWriteIndexNameForAliasCallCount
        )
        assertEquals("Alias queried should match monitor queryIndex alias", aliasName, testableQueries.lastAliasQueried)
        assertEquals(
            "Metadata mapping should be set to resolved concrete index",
            concreteResolvedIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should receive the resolved concrete index, not the alias",
            concreteResolvedIndex,
            testableQueries.lastPutConcreteQueryIndex
        )
    }

    // getWriteIndexNameForAlias must return null when the alias exists but no write index is designated.
    fun `test getWriteIndexNameForAlias returns null when alias exists but has no write index designated`() {
        val aliasName = "test-query-alias"
        val concreteIndex = "concrete-index-000001"

        // writeIndex(false) means this backing index is explicitly NOT the write index
        val indexMetadata = IndexMetadata.builder(concreteIndex)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(aliasName).writeIndex(false).build())
            .build()
        val metadata = Metadata.builder().put(indexMetadata, false).build()
        val clusterState = ClusterState.builder(ClusterName("test-cluster")).metadata(metadata).build()
        whenever(clusterService.state()).thenReturn(clusterState)

        val docLevelMonitorQueries = DocLevelMonitorQueries(client, clusterService)
        val writeIndex = docLevelMonitorQueries.getWriteIndexNameForAlias(aliasName)

        assertNull(
            "Expected null when alias has no write index designated (writeIndex=false on all backing indices)",
            writeIndex
        )
    }

    // putMapping throws field-limit exception, rollover is triggered, returned targetIndex is the new rolled-over name.
    fun `test updateQueryIndexMappings when putMapping throws field-limit exception triggers rollover and returns new index`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = true)
        val sourceIndex = "test-source-index"
        val initialConcreteIndex = "concrete-query-index-000001"
        val rolledOverIndex = "concrete-query-index-000002"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = initialConcreteIndex
        )

        val testableQueries = TestableDocLevelMonitorQueries(
            client,
            clusterService,
            resolvedIndexName = initialConcreteIndex,
            rolledOverIndexName = rolledOverIndex
        )

        // First putMapping call triggers field-limit failure; second (post-rollover) call succeeds.
        var putMappingCallCount = 0
        Mockito.doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            putMappingCallCount++
            if (putMappingCallCount == 1) {
                // Wrap in RemoteTransportException to simulate what OpenSearch sends back over the wire
                listener.onFailure(RemoteTransportException("field-limit-error", RuntimeException("Limit of total fields [1000] has been exceeded")))
            } else {
                listener.onResponse(AcknowledgedResponse(true))
            }
            null
        }.`when`(indicesAdminClient).putMapping(any(PutMappingRequest::class.java), any())

        val (response, targetIndex) = runBlocking {
            testableQueries.updateQueryIndexMappings(
                monitor = monitor,
                monitorMetadata = metadata,
                sourceIndex = sourceIndex,
                sourceIndexFieldLimit = 1000L,
                updatedProperties = mutableMapOf(),
                indexTimeout = TimeValue.timeValueSeconds(30)
            )
        }

        assertTrue("Expected update mapping response to be acknowledged after rollover", response.isAcknowledged)
        assertEquals("Expected target index to be the new rolled-over index after rollover", rolledOverIndex, targetIndex)
        assertEquals(
            "Metadata should be updated to the new rolled-over index",
            rolledOverIndex,
            metadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        )
        assertEquals(
            "adjustMaxFieldLimitForQueryIndex should have been called last with the rolled-over index",
            rolledOverIndex,
            testableQueries.lastPutConcreteQueryIndex
        )
    }

    // putMapping throws a non-field-limit exception with deleteQueryIndexInEveryRun=false — expect AlertingException.
    fun `test updateQueryIndexMappings when putMapping throws non-field-limit exception and deleteQueryIndexInEveryRun false throws AlertingException`() {
        val aliasName = "query-index-alias"
        val monitor = createMonitor(queryIndexAlias = aliasName, deleteQueryIndexInEveryRun = false)
        val sourceIndex = "test-source-index"
        val initialConcreteIndex = "concrete-query-index-000001"
        val metadata = createMonitorMetadata(
            monitorId = monitor.id,
            sourceIndex = sourceIndex,
            initialTargetQueryIndex = initialConcreteIndex
        )

        val testableQueries = TestableDocLevelMonitorQueries(
            client,
            clusterService,
            resolvedIndexName = initialConcreteIndex
        )

        // putMapping throws a non-field-limit exception
        Mockito.doAnswer { invocation ->
            @Suppress("UNCHECKED_CAST")
            val listener = invocation.arguments[1] as ActionListener<AcknowledgedResponse>
            listener.onFailure(RemoteTransportException("generic-error", RuntimeException("Some unexpected mapping error")))
            null
        }.`when`(indicesAdminClient).putMapping(any(PutMappingRequest::class.java), any())

        assertFailsWith<AlertingException>(
            message = "Expected AlertingException when putMapping fails with a non-field-limit error and deleteQueryIndexInEveryRun is false"
        ) {
            runBlocking {
                testableQueries.updateQueryIndexMappings(
                    monitor = monitor,
                    monitorMetadata = metadata,
                    sourceIndex = sourceIndex,
                    sourceIndexFieldLimit = 1000L,
                    updatedProperties = mutableMapOf(),
                    indexTimeout = TimeValue.timeValueSeconds(30)
                )
            }
        }
    }

    // -------------------------------------------------------------------------
    // isUnrecoverableMappingError predicate (updateQueryIndexMappings fast-fail)
    //
    // The predicate is inlined as a local val inside the private suspend fun
    // updateQueryIndexMappings and cannot be invoked directly.  We test the
    // boolean expression directly here to protect against message-format drift
    // and to document which error strings trigger the fast-fail path.
    //
    // Production predicate (DocLevelMonitorQueries.kt lines 589-592):
    //   it.contains("alias must refer to an existing field") ||
    //     (it.contains("mapper [") && it.contains("cannot be changed from type"))
    // -------------------------------------------------------------------------

    private fun isUnrecoverableMappingError(msg: String?): Boolean =
        msg?.let {
            it.contains("alias must refer to an existing field") ||
                (it.contains("mapper [") && it.contains("cannot be changed from type"))
        } ?: false

    fun `test isUnrecoverableMappingError matches alias-refer-to-existing-field message`() {
        val msg = "alias must refer to an existing field [real_field]"

        assertTrue(
            "Expected 'alias must refer to an existing field' to trigger the fast-fail predicate",
            isUnrecoverableMappingError(msg),
        )
    }

    fun `test isUnrecoverableMappingError matches mapper-cannot-be-changed-from-type message`() {
        val msg = "mapper [timestamp] cannot be changed from type [date] to [keyword]"

        assertTrue(
            "Expected 'mapper [X] cannot be changed from type' to trigger the fast-fail predicate",
            isUnrecoverableMappingError(msg),
        )
    }

    fun `test isUnrecoverableMappingError does not match unrelated error message`() {
        val msg = "Limit of total fields [1000] has been exceeded"

        assertFalse(
            "Expected field-limit error to NOT trigger the fast-fail predicate (it has a separate retry path)",
            isUnrecoverableMappingError(msg),
        )
    }

    fun `test isUnrecoverableMappingError returns false for null message`() {
        assertFalse(
            "Expected null message to return false from the fast-fail predicate",
            isUnrecoverableMappingError(null),
        )
    }

    fun `test isUnrecoverableMappingError requires both mapper-tokens for type-change branch`() {
        // "mapper [" alone without "cannot be changed from type" must NOT match.
        val msgMapperOnly = "mapper [field] is already defined"
        // "cannot be changed from type" alone without "mapper [" must NOT match.
        val msgPhraseOnly = "cannot be changed from type [date] to [keyword]"

        assertFalse(
            "Only 'mapper [' substring must NOT trigger the predicate",
            isUnrecoverableMappingError(msgMapperOnly),
        )
        assertFalse(
            "Only 'cannot be changed from type' must NOT trigger the predicate",
            isUnrecoverableMappingError(msgPhraseOnly),
        )
    }


    fun `test native wildcard mapping retains source parameters`() {
        val mapping = mapOf<String, Any>(
            "type" to "wildcard",
            "doc_values" to false,
            "null_value" to "missing",
            "ignore_above" to 4096,
            "normalizer" to "custom",
            "meta" to mapOf("owner" to "test")
        )
        val result = DocLevelMonitorQueries.applyQueryIndexMappingOverrides(
            mapping, mapOf("text" to mapOf("analyzer" to "rule_analyzer"))
        )
        assertEquals(mapping, result)
        assertEquals("wildcard", mapping["type"])
        assertEquals(false, mapping["doc_values"])
    }

    fun `test same type overrides retain compatible source parameters`() {
        val result = DocLevelMonitorQueries.applyQueryIndexMappingOverrides(
            mapOf("type" to "text", "store" to true, "analyzer" to "custom"),
            mapOf("text" to mapOf("analyzer" to "rule_analyzer"))
        )
        assertEquals(mapOf("type" to "text", "store" to true, "analyzer" to "rule_analyzer"), result)
    }

    fun `test unconfigured field types retain original mapping`() {
        val mapping = mapOf<String, Any>("type" to "keyword", "ignore_above" to 512)
        assertEquals(mapping, DocLevelMonitorQueries.applyQueryIndexMappingOverrides(mapping, emptyMap()))
    }

    fun `test direct query field rewrite respects field boundaries`() {
        assertEquals(
            "message_index_monitor:*success* AND other.message:*failure* AND longer_message:*failure*",
            DocLevelMonitorQueries.rewriteQueryField(
                "message:*success* AND other.message:*failure* AND longer_message:*failure*", "message", "_index_monitor"
            )
        )
    }

    @Suppress("UNCHECKED_CAST")
    fun `test text multifields receive analyzer override without changing native wildcard parent`() {
        val mapping = mapOf<String, Any>(
            "type" to "wildcard",
            "fields" to mapOf("text" to mapOf("type" to "text"))
        )
        val result = DocLevelMonitorQueries.applyQueryIndexMappingOverrides(
            mapping, mapOf("text" to mapOf("analyzer" to "rule_analyzer"))
        )
        val fields = result["fields"] as Map<String, Any>
        assertEquals("wildcard", result["type"])
        assertEquals(mapOf("type" to "text", "analyzer" to "rule_analyzer"), fields["text"])
        assertEquals(mapOf("type" to "text"), (mapping["fields"] as Map<String, Any>)["text"])
        assertEquals(
            "attributes.message.wildcard:*success* AND other.attributes.message:*failure*",
            DocLevelMonitorQueries.rewriteQueryField(
                "attributes.message.wildcard:*success* AND other.attributes.message:*failure*",
                "attributes.message", "_index_monitor"
            )
        )
    }

    fun `test native DSL rewrites field keys and exists without changing literal values`() {
        val query = """{"bool":{"must":[{"wildcard":{"message":{"value":"*message:literal_ws_*"}}},
            {"exists":{"field":"message.raw"}},{"query_string":{"query":"action:login AND note:\"message:literal\"","fields":["action^2"]}}]}}"""
        val actual = DocLevelMonitorQueries.queryForPercolation(
            query, mapOf("message" to "message_suffix", "message.raw" to "message_suffix.raw", "action" to "action_suffix"), emptyList()
        )
        assertEquals(
            mapOf(
                "bool" to mapOf(
                    "must" to listOf(
                        mapOf("wildcard" to mapOf("message_suffix" to mapOf("value" to "*message:literal_ws_*"))),
                        mapOf("exists" to mapOf("field" to "message_suffix.raw")),
                        mapOf(
                            "query_string" to mapOf(
                                "query" to "action_suffix:login AND note:\"message:literal\"",
                                "fields" to listOf("action_suffix^2")
                            )
                        )
                    )
                )
            ),
            actual
        )
    }

    fun `test query string rewrite preserves quoted and escaped values and rewrites exists`() {
        assertEquals(
            "message_suffix:value AND note:\"message:literal\" AND note:message\\:literal AND _exists_:message_suffix.raw",
            DocLevelMonitorQueries.rewriteQueryString(
                "message:value AND note:\"message:literal\" AND note:message\\:literal AND _exists_:message.raw",
                mapOf("message" to "message_suffix", "message.raw" to "message_suffix.raw")
            )
        )
    }

    fun `test malformed native JSON is rejected rather than used as a legacy query`() {
        expectThrows(Exception::class.java) {
            DocLevelMonitorQueries.queryForPercolation("{invalid", emptyMap(), emptyList())
        }
    }

    fun `test native percolator variants are scoped to exact concrete index and rule ids`() {
        val queries = listOf(
            DocLevelQuery("native_rule", "native", emptyList(), """{"bool":{"must_not":{"exists":{"field":"foo"}}}}"""),
            DocLevelQuery("legacy_rule", "legacy", emptyList(), "NOT foo:*")
        )
        assertEquals(
            "index_b",
            DocLevelMonitorQueries.nativeQueryConcreteIndex(
                "native_rule_index_b_monitor", "monitor", queries, listOf("index_a", "index_b")
            )
        )
        assertNull(
            DocLevelMonitorQueries.nativeQueryConcreteIndex(
                "legacy_rule_index_b_monitor", "monitor", queries, listOf("index_a", "index_b")
            )
        )
        assertNull(
            DocLevelMonitorQueries.nativeQueryConcreteIndex(
                "other_native_rule_index_b_monitor", "monitor", queries, listOf("index_a", "index_b")
            )
        )
    }

    fun `test mapping metadata distinguishes multifields from object children`() {
        val scalar = DocLevelMonitorQueries.sourceFieldParents(
            mapOf(
                "properties" to mapOf(
                    "foo" to mapOf("type" to "wildcard", "fields" to mapOf("raw" to mapOf("type" to "keyword")))
                )
            )
        )
        val objectMapping = DocLevelMonitorQueries.sourceFieldParents(
            mapOf(
                "properties" to mapOf(
                    "foo" to mapOf("properties" to mapOf("raw" to mapOf("type" to "wildcard")))
                )
            )
        )
        assertEquals(mapOf("foo" to "foo", "foo.raw" to "foo"), scalar)
        assertEquals(mapOf("foo.raw" to "foo.raw"), objectMapping)
    }

}
