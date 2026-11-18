/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.ExceptionsHelper
import org.opensearch.OpenSearchStatusException
import org.opensearch.ResourceAlreadyExistsException
import org.opensearch.action.admin.indices.alias.Alias
import org.opensearch.action.admin.indices.create.CreateIndexRequest
import org.opensearch.action.admin.indices.create.CreateIndexResponse
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest
import org.opensearch.action.admin.indices.rollover.RolloverRequest
import org.opensearch.action.admin.indices.rollover.RolloverResponse
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.bulk.BulkResponse
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.WriteRequest.RefreshPolicy
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.alerting.MonitorRunnerService.monitorCtx
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.DataSources
import org.opensearch.commons.alerting.model.DocLevelMonitorInput
import org.opensearch.commons.alerting.model.DocLevelQuery
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.DeprecationHandler
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.BulkByScrollResponse
import org.opensearch.index.reindex.DeleteByQueryAction
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder
import org.opensearch.transport.client.Client
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

private val log = LogManager.getLogger(DocLevelMonitorQueries::class.java)

/**
 * Manages doc-level query index lifecycle: creation, mapping updates, rollover, and field-limit adjustment.
 * `open` for test isolation only — not designed as a production extension point.
 */
open class DocLevelMonitorQueries(private val client: Client, private val clusterService: ClusterService) {
    companion object {

        const val PROPERTIES = "properties"
        const val NESTED = "nested"
        const val TYPE = "type"
        const val INDEX_PATTERN_SUFFIX = "-000001"
        const val QUERY_INDEX_BASE_FIELDS_COUNT = 8 // 3 fields we defined and 5 builtin additional metadata fields

        @JvmStatic
        fun docLevelQueriesMappings(): String {
            return DocLevelMonitorQueries::class.java.classLoader.getResource("mappings/doc-level-queries.json").readText()
        }
        fun docLevelQueriesSettings(): Settings {
            return Settings.builder().loadFromSource(
                DocLevelMonitorQueries::class.java.classLoader.getResource("settings/doc-level-queries.json").readText(),
                XContentType.JSON
            ).build()
        }

        /**
         * Field attributes that reference custom analysis resources (analyzers, normalizers, similarity
         * configurations) defined in a source index's settings.analysis block.  The doc-level query index
         * is created from a fixed settings resource with no custom analysis block, so including these
         * attributes in a PutMappingRequest against the query index causes OpenSearch to reject the
         * request with "analyzer [x] has not been configured in mappings".  Strip them before copying
         * field mappings from the source index to the query index.
         */
        val ANALYSIS_ATTRIBUTES = setOf(
            "analyzer",
            "search_analyzer",
            "search_quote_analyzer",
            "normalizer",
            "similarity"
        )

        /**
         * Sanitizes a single field-mapping property map so it is safe to submit in a PutMappingRequest
         * against the doc-level query index.
         * @param fieldType the OpenSearch field type string (e.g. "text", "keyword"), or null when absent
         * @param mapping   the mutable property map for the field; modified in-place
         */
        fun sanitizeFieldMappingAttributes(fieldType: String?, mapping: MutableMap<String, Any>) {
            // Category 1: remove analysis resource references
            mapping.keys.removeAll(ANALYSIS_ATTRIBUTES)

            // Category 2: remove "properties" from scalar (non-object, non-nested) fields.
            // A null/absent type defaults to "object" in OpenSearch, so only strip when the type is
            // explicitly set to something other than "object" or "nested".
            if (fieldType != null && fieldType != "object" && fieldType != NESTED) {
                mapping.remove(PROPERTIES)
            }

            // Recurse into multi-fields
            @Suppress("UNCHECKED_CAST")
            (mapping["fields"] as? Map<*, *>)?.forEach { (_, subMapping) ->
                (subMapping as? MutableMap<String, Any>)?.let {
                    sanitizeFieldMappingAttributes(it[TYPE] as? String, it)
                }
            }

            // Recurse into sub-properties of object/implicit-object fields so that analysis
            // attributes on nested leaves are stripped before the PutMappingRequest is issued.
            if (fieldType == null || fieldType == "object" || fieldType == NESTED) {
                @Suppress("UNCHECKED_CAST")
                (mapping[PROPERTIES] as? Map<*, *>)?.forEach { (_, subMapping) ->
                    (subMapping as? MutableMap<String, Any>)?.let {
                        sanitizeFieldMappingAttributes(it[TYPE] as? String, it)
                    }
                }
            }
        }

        /** Applies configured overrides recursively, including multifields. */
        @Suppress("UNCHECKED_CAST")
        fun applyQueryIndexMappingOverrides(
            mapping: Map<String, Any>,
            mappingsByType: Map<String, Map<String, String>>
        ): MutableMap<String, Any> {
            val overrides = mappingsByType[mapping[TYPE]] ?: emptyMap()
            val result = deepCopyMap(mapping)
            result.putAll(overrides)
            (result["fields"] as? Map<String, Any>)?.let { fields ->
                result["fields"] = fields.mapValues { (_, field) ->
                    applyQueryIndexMappingOverrides(field as Map<String, Any>, mappingsByType)
                }.toMutableMap()
            }
            return result
        }

        /** Rewrites an exact legacy field token, never an arbitrary dotted prefix. */
        fun rewriteQueryField(query: String, fieldPath: String, suffix: String): String {
            return rewriteQueryString(query, mapOf(fieldPath to "$fieldPath$suffix"))
        }

        /** Native query variants are scoped to the concrete mapping used to compile them. */
        fun nativeQueryConcreteIndex(
            hitId: String,
            monitorId: String,
            queries: List<DocLevelQuery>,
            concreteIndices: List<String>
        ): String? {
            return concreteIndices.firstOrNull { index ->
                queries.any { it.query.trimStart().startsWith("{") && hitId == "${it.id}_${index}_$monitorId" }
            }
        }

        /** Maps actual multifields to the source leaf whose value indexes them. */
        @Suppress("UNCHECKED_CAST")
        fun sourceFieldParents(mapping: Map<String, Any>): Map<String, String> {
            val parents = mutableMapOf<String, String>()
            val aliases = mutableMapOf<String, String>()
            fun visit(fields: Map<String, Any>, prefix: String, parent: String? = null) {
                fields.forEach { (name, value) ->
                    val props = value as? Map<String, Any> ?: return@forEach
                    val path = if (prefix.isEmpty()) name else "$prefix.$name"
                    if (props[TYPE] == "alias") aliases[path] = props["path"] as String
                    else if (props[PROPERTIES] is Map<*, *>) visit(props[PROPERTIES] as Map<String, Any>, path)
                    else {
                        parents[path] = parent ?: path
                        (props["fields"] as? Map<String, Any>)?.let { visit(it, path, parent ?: path) }
                    }
                }
            }
            visit(mapping[PROPERTIES] as? Map<String, Any> ?: emptyMap(), "")
            aliases.forEach { (alias, target) ->
                var resolved = target
                val seen = mutableSetOf(alias)
                while (aliases.containsKey(resolved) && seen.add(resolved)) resolved = aliases.getValue(resolved)
                parents[resolved]?.let { parents[alias] = it }
            }
            return parents
        }

        /** Only field tokens outside quoted values are rewritten. */
        fun rewriteQueryString(query: String, fields: Map<String, String>): String {
            val out = StringBuilder()
            var i = 0
            var quote: Char? = null
            while (i < query.length) {
                val c = query[i]
                if (c == '\\' && i + 1 < query.length) { out.append(c).append(query[i + 1]); i += 2; continue }
                if (c == '"' || c == '\'') { if (quote == c) quote = null else if (quote == null) quote = c }
                val fieldBoundary = i == 0 || query[i - 1].isWhitespace() || query[i - 1] in "(+-"
                if (quote == null && fieldBoundary && (c.isLetterOrDigit() || c == '_' || c == '@')) {
                    val start = i
                    while (i < query.length && (query[i].isLetterOrDigit() || query[i] in "_.-@")) i++
                    val token = query.substring(start, i)
                    if (i < query.length && query[i] == ':') {
                        out.append(fields[token] ?: token).append(':'); i++
                        if (token == "_exists_") {
                            while (i < query.length && query[i].isWhitespace()) out.append(query[i++])
                            val valueStart = i
                            while (i < query.length && (query[i].isLetterOrDigit() || query[i] in "_.-@")) i++
                            val value = query.substring(valueStart, i)
                            out.append(fields[value] ?: value)
                        }
                    } else out.append(token)
                } else { out.append(c); i++ }
            }
            return out.toString()
        }

        @Suppress("UNCHECKED_CAST")
        fun queryForPercolation(query: String, fields: Map<String, String>, defaultFields: List<String>): Map<String, Any> {
            if (!query.trimStart().startsWith("{")) {
                return mapOf("query_string" to mapOf("query" to rewriteQueryString(query, fields), "fields" to defaultFields))
            }
            val parsed = XContentType.JSON.xContent().createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, query
            ).use { it.map() }
            fun rewrite(node: Map<String, Any>): Map<String, Any> = node.mapValues { (kind, value) ->
                when (kind) {
                    "wildcard", "term", "terms", "match", "match_phrase", "prefix", "regexp", "range", "fuzzy" ->
                        (value as Map<String, Any>).mapKeys { fields[it.key] ?: it.key }
                    "exists" -> (value as Map<String, Any>).mapValues { (key, v) -> if (key == "field") fields[v] ?: v else v }
                    "query_string", "simple_query_string" -> (value as Map<String, Any>).mapValues { (key, v) ->
                        when (key) {
                            "query" -> rewriteQueryString(v as String, fields)
                            "default_field" -> fields[v] ?: v
                            "fields" -> (v as List<String>).map { field ->
                                val parts = field.split('^', limit = 2)
                                (fields[parts[0]] ?: parts[0]) + if (parts.size == 2) "^${parts[1]}" else ""
                            }
                            else -> v
                        }
                    }
                    "bool" -> (value as Map<String, Any>).mapValues { (key, v) ->
                        if (key in setOf("must", "should", "filter", "must_not")) {
                            if (v is List<*>) v.map { rewrite(it as Map<String, Any>) } else rewrite(v as Map<String, Any>)
                        } else v
                    }
                    "constant_score" -> (value as Map<String, Any>).mapValues { (key, v) ->
                        if (key == "filter") rewrite(v as Map<String, Any>) else v
                    }
                    else -> value
                }
            }
            return rewrite(parsed)
        }

        /**
         * Returns a deep copy of [source] such that every nested [Map] is a distinct [MutableMap] and
         * every [List] is a distinct [MutableList] whose [Map] elements are themselves deep-copied.
         *
         * This prevents aliasing bugs where mutations to the copy (e.g. leaf-name renaming during
         * [traverseMappingsAndUpdate]) would otherwise silently corrupt the original mapping tree
         * read from cluster-state metadata.
         */
        @Suppress("UNCHECKED_CAST")
        fun deepCopyMap(source: Map<String, Any>): MutableMap<String, Any> {
            val copy = mutableMapOf<String, Any>()
            for ((key, value) in source) {
                copy[key] = deepCopyValue(value)
            }
            return copy
        }

        @Suppress("UNCHECKED_CAST")
        private fun deepCopyValue(value: Any): Any {
            return when (value) {
                is Map<*, *> -> deepCopyMap(value as Map<String, Any>)
                is List<*> -> {
                    // Mapping trees do not contain null elements, but guard defensively rather than crashing.
                    value.mapNotNull { element -> deepCopyValue(element ?: return@mapNotNull null) }
                        .toMutableList()
                }
                else -> value // scalars (String, Int, Long, Boolean, …) are immutable — no copy needed
            }
        }
    }

    suspend fun initDocLevelQueryIndex(): Boolean {
        if (!docLevelQueryIndexExists()) {
            // Since we changed queryIndex to be alias now, for backwards compatibility, we have to delete index with same name
            // as our alias, to avoid name clash.
            if (clusterService.state().metadata.hasIndex(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)) {
                val acknowledgedResponse: AcknowledgedResponse = client.suspendUntil {
                    admin().indices().delete(DeleteIndexRequest(ScheduledJob.DOC_LEVEL_QUERIES_INDEX), it)
                }
                if (!acknowledgedResponse.isAcknowledged) {
                    val errorMessage = "Deletion of old queryIndex [${ScheduledJob.DOC_LEVEL_QUERIES_INDEX}] index is not acknowledged!"
                    log.error(errorMessage)
                    throw AlertingException.wrap(OpenSearchStatusException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR))
                }
            }
            val alias = ScheduledJob.DOC_LEVEL_QUERIES_INDEX
            val indexPattern = ScheduledJob.DOC_LEVEL_QUERIES_INDEX + INDEX_PATTERN_SUFFIX
            val indexRequest = CreateIndexRequest(indexPattern)
                .mapping(docLevelQueriesMappings())
                .alias(Alias(alias))
                .settings(docLevelQueriesSettings())
            return try {
                val createIndexResponse: CreateIndexResponse = client.suspendUntil { client.admin().indices().create(indexRequest, it) }
                createIndexResponse.isAcknowledged
            } catch (t: Exception) {
                if (ExceptionsHelper.unwrapCause(t) is ResourceAlreadyExistsException) {
                    true
                } else {
                    throw t
                }
            }
        }
        return true
    }
    suspend fun initDocLevelQueryIndex(dataSources: DataSources): Boolean {
        if (dataSources.queryIndex == ScheduledJob.DOC_LEVEL_QUERIES_INDEX) {
            return initDocLevelQueryIndex()
        }
        // Since we changed queryIndex to be alias now, for backwards compatibility, we have to delete index with same name
        // as our alias, to avoid name clash.
        if (clusterService.state().metadata.hasIndex(dataSources.queryIndex)) {
            val acknowledgedResponse: AcknowledgedResponse = client.suspendUntil {
                admin().indices().delete(DeleteIndexRequest(dataSources.queryIndex), it)
            }
            if (!acknowledgedResponse.isAcknowledged) {
                log.warn("Deletion of old queryIndex [${dataSources.queryIndex}] index is not acknowledged!")
            }
        }
        val alias = dataSources.queryIndex
        val indexPattern = dataSources.queryIndex + INDEX_PATTERN_SUFFIX
        if (!clusterService.state().metadata.hasAlias(alias)) {
            val indexRequest = CreateIndexRequest(indexPattern)
                .mapping(docLevelQueriesMappings())
                .alias(Alias(alias))
                .settings(
                    Settings.builder().put("index.hidden", true)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                        .build()
                )
            return try {
                val createIndexResponse: CreateIndexResponse = client.suspendUntil { client.admin().indices().create(indexRequest, it) }
                createIndexResponse.isAcknowledged
            } catch (t: Exception) {
                if (ExceptionsHelper.unwrapCause(t) is ResourceAlreadyExistsException) {
                    true
                } else {
                    throw t
                }
            }
        }
        return true
    }

    suspend fun deleteDocLevelQueriesOnDryRun(monitorMetadata: MonitorMetadata) {
        try {
            monitorMetadata.sourceToQueryIndexMapping.forEach { (_, queryIndex) ->
                val indicesExistsResponse: IndicesExistsResponse =
                    client.suspendUntil {
                        client.admin().indices().exists(IndicesExistsRequest(queryIndex), it)
                    }
                if (indicesExistsResponse.isExists == false) {
                    return@forEach
                }

                val queryBuilder = QueryBuilders.boolQuery()
                    .must(QueryBuilders.existsQuery("monitor_id"))
                    .mustNot(QueryBuilders.wildcardQuery("monitor_id", "*"))

                val response: BulkByScrollResponse = suspendCoroutine { cont ->
                    DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                        .source(queryIndex)
                        .filter(queryBuilder)
                        .refresh(true)
                        .execute(
                            object : ActionListener<BulkByScrollResponse> {
                                override fun onResponse(response: BulkByScrollResponse) = cont.resume(response)
                                override fun onFailure(t: Exception) = cont.resumeWithException(t)
                            }
                        )
                }
                response.bulkFailures.forEach {
                    log.error("Failed deleting queries while removing dry run queries: [${it.id}] cause: [${it.cause}] ")
                }
            }
        } catch (e: Exception) {
            log.error("Failed to delete doc level queries on dry run", e)
        }
    }

    suspend fun deleteDocLevelQueryIndex(dataSources: DataSources): Boolean {
        val ack: AcknowledgedResponse = client.suspendUntil {
            client.admin().indices().delete(
                DeleteIndexRequest(dataSources.queryIndex).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN),
                it
            )
        }
        return ack.isAcknowledged
    }

    fun docLevelQueryIndexExists(dataSources: DataSources): Boolean {
        val clusterState = clusterService.state()
        return clusterState.metadata.hasAlias(dataSources.queryIndex)
    }

    fun docLevelQueryIndexExists(): Boolean {
        val clusterState = clusterService.state()
        return clusterState.metadata.hasAlias(ScheduledJob.DOC_LEVEL_QUERIES_INDEX)
    }

    /**
     * Does a DFS traversal of index mappings tree.
     * Calls processLeafFn on every leaf node.
     * Populates flattenPaths list with full paths of leaf nodes
     * @param node current node which we're visiting
     * @param currentPath current node path from root node
     * @param processLeafFn leaf processor function which is called on every leaf discovered
     * @param flattenPaths list of full paths of all leaf nodes relative to root
     */
    fun traverseMappingsAndUpdate(
        node: MutableMap<String, Any>,
        currentPath: String,
        processLeafFn: (String, String, MutableMap<String, Any>) -> Triple<String, String, MutableMap<String, Any>>,
        flattenPaths: MutableMap<String, MutableMap<String, Any>>
    ) {
        // If node contains "properties" property then it is internal(non-leaf) node
        log.debug("Node in traverse: $node")
        // newNodes will hold list of updated leaf properties
        val newNodes = ArrayList<Triple<String, String, Any>>(node.size)
        node.entries.forEach {
            // Compute full path relative to root
            val fullPath = if (currentPath.isEmpty()) it.key
            else "$currentPath.${it.key}"
            val nodeProps = (it.value as Map<String, Any>).toMutableMap()
            // If it has type property and type is not "nested" then this is a leaf
            if (nodeProps.containsKey(TYPE) && nodeProps[TYPE] != NESTED) {
                // At this point we know full path of node, so we add it to output array
                flattenPaths.put(fullPath, nodeProps)
                // Calls processLeafFn and gets old node name, new node name and new properties of node.
                // This is all information we need to update this node
                // Reuse nodeProps rather than creating a second independent copy from it.value.
                val (oldName, newName, props) = processLeafFn(it.key, fullPath, nodeProps)
                newNodes.add(Triple(oldName, newName, props))
            } else if (nodeProps.containsKey(PROPERTIES) && nodeProps[PROPERTIES] != null) {
                // Internal(non-leaf) node - visit children.
                // Copy nodeProps[PROPERTIES] into childProps, recurse on childProps, then write
                // childProps back into nodeProps[PROPERTIES] so the parent node sees updated children.
                @Suppress("UNCHECKED_CAST")
                val childProps = (nodeProps[PROPERTIES] as Map<String, Any>).toMutableMap()
                traverseMappingsAndUpdate(childProps, fullPath, processLeafFn, flattenPaths)
                // Write the mutated child properties back so the parent node sees the updates.
                nodeProps[PROPERTIES] = childProps
                newNodes.add(Triple(it.key, it.key, nodeProps as Any))
            }
        }
        // Here we can update all processed leaves in tree
        newNodes.forEach {
            // If we renamed leaf, we have to remove it first
            if (it.first != it.second) {
                node.remove(it.first)
            }
            // Put new properties of leaf
            node.put(it.second, it.third)
        }
    }

    suspend fun indexDocLevelQueries(
        monitor: Monitor,
        monitorId: String,
        monitorMetadata: MonitorMetadata,
        refreshPolicy: RefreshPolicy = RefreshPolicy.IMMEDIATE,
        indexTimeout: TimeValue,
        isRetry: Boolean = false
    ) {
        val docLevelMonitorInput = monitor.inputs[0] as DocLevelMonitorInput
        val queries: List<DocLevelQuery> = docLevelMonitorInput.queries

        val indices = docLevelMonitorInput.indices
        val clusterState = clusterService.state()

        // Run through each backing index and apply appropriate mappings to query index
        indices.forEach { indexName ->
            val concreteIndices = IndexUtils.resolveAllIndices(
                listOf(indexName),
                monitorCtx.clusterService!!,
                monitorCtx.indexNameExpressionResolver!!
            )
            // Note: no creation-date truncation here. We must inspect ALL backing indices so that
            // getAllConflictingFields can detect fields whose type changed across rollover boundaries
            // (e.g. resource.host.name: text → keyword). Filtering out old indices hides the conflict,
            // causing a merged putMapping with a type change that OpenSearch core rejects. Seq_no
            // watermarks in the runner guarantee that sealed indices produce empty document fan-outs
            // at negligible cost.
            val updatedIndexName = indexName.replace("*", "_")
            val updatedProperties = mutableMapOf<String, Any>()
            val allFlattenPaths = mutableSetOf<Pair<String, String>>()
            val fieldParentsByIndex = mutableMapOf<String, Map<String, String>>()
            var sourceIndexFieldLimit = 0L
            val conflictingFields = getAllConflictingFields(clusterState, concreteIndices)

            concreteIndices.forEach { concreteIndexName ->
                if (clusterState.routingTable.hasIndex(concreteIndexName)) {
                    val indexMetadata = clusterState.metadata.index(concreteIndexName)
                    if (indexMetadata.mapping()?.sourceAsMap?.get("properties") != null) {
                        fieldParentsByIndex[concreteIndexName] = sourceFieldParents(indexMetadata.mapping()!!.sourceAsMap)
                        @Suppress("UNCHECKED_CAST")
                        val properties = deepCopyMap(
                            indexMetadata.mapping()?.sourceAsMap?.get("properties") as Map<String, Any>
                        )
                        val leafNodeProcessor = buildLeafNodeProcessor(
                            monitorId = monitorId,
                            concreteIndexName = concreteIndexName,
                            updatedIndexName = updatedIndexName,
                            conflictingFields = conflictingFields,
                            queryIndexMappingsByType = monitor.dataSources.queryIndexMappingsByType
                        )
                        // Traverse and update index mappings here while extracting flatten field paths
                        val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
                        traverseMappingsAndUpdate(properties, "", leafNodeProcessor, flattenPaths)
                        flattenPaths.entries
                            .filter { (_, props) -> props["type"] != "alias" }
                            .forEach { (path, _) -> allFlattenPaths.add(Pair(path, concreteIndexName)) }
                        // Updated mappings ready to be applied on queryIndex
                        properties.forEach {
                            if (
                                it.value is Map<*, *> &&
                                (it.value as Map<String, Any>).containsKey("type") &&
                                (it.value as Map<String, Any>)["type"] == NESTED
                            ) {
                                // skip nested — handled by traverseMappingsAndUpdate
                            } else {
                                if (updatedProperties.containsKey(it.key) && updatedProperties[it.key] != it.value) {
                                    val mergedField = mergeConflictingFields(
                                        updatedProperties[it.key] as Map<String, Any>,
                                        it.value as Map<String, Any>
                                    )
                                    updatedProperties[it.key] = mergedField
                                } else {
                                    updatedProperties[it.key] = it.value
                                }
                            }
                        }
                        sourceIndexFieldLimit += checkMaxFieldLimit(concreteIndexName)
                    }
                }
            }
            // Updates mappings of concrete queryIndex. This can rollover queryIndex if field mapping limit is reached.
            val (updateMappingResponse, concreteQueryIndex) = updateQueryIndexMappings(
                monitor,
                monitorMetadata,
                updatedIndexName,
                sourceIndexFieldLimit,
                updatedProperties,
                indexTimeout,
                refreshPolicy,
                isRetry
            )

            if (updateMappingResponse.isAcknowledged) {
                doIndexAllQueries(
                    concreteQueryIndex,
                    updatedIndexName,
                    monitorId,
                    queries,
                    allFlattenPaths,
                    conflictingFields,
                    fieldParentsByIndex,
                    refreshPolicy,
                    indexTimeout
                )
            }
        }
    }

    /**
     * Builds the leaf-node renaming processor for [traverseMappingsAndUpdate], suffixing conflicting
     * field names and alias target paths with [concreteIndexName] and non-conflicting ones with
     * [updatedIndexName].
     */
    fun buildLeafNodeProcessor(
        monitorId: String,
        concreteIndexName: String,
        updatedIndexName: String,
        conflictingFields: Set<String>,
        queryIndexMappingsByType: Map<String, Map<String, String>> = emptyMap()
    ): (String, String, MutableMap<String, Any>) -> Triple<String, String, MutableMap<String, Any>> {
        return fun(
            fieldName: String,
            fullPath: String,
            props: MutableMap<String, Any>
        ): Triple<String, String, MutableMap<String, Any>> {
            val newProps = applyQueryIndexMappingOverrides(props, queryIndexMappingsByType)
            val fieldSuffix = if (conflictingFields.contains(fullPath)) concreteIndexName else updatedIndexName
            if (props.containsKey("path")) {
                val targetPath = props["path"] as? String
                if (targetPath != null) {
                    val targetSuffix = if (conflictingFields.contains(targetPath)) concreteIndexName else updatedIndexName
                    newProps["path"] = "${targetPath}_${targetSuffix}_$monitorId"
                }
            }
            return Triple(fieldName, "${fieldName}_${fieldSuffix}_$monitorId", newProps)
        }
    }

    private suspend fun doIndexAllQueries(
        concreteQueryIndex: String,
        sourceIndex: String,
        monitorId: String,
        queries: List<DocLevelQuery>,
        flattenPaths: MutableSet<Pair<String, String>>,
        conflictingPaths: Set<String>,
        fieldParentsByIndex: Map<String, Map<String, String>>,
        refreshPolicy: RefreshPolicy,
        indexTimeout: TimeValue
    ) {
        val indexRequests = mutableListOf<IndexRequest>()
        val conflictingPathToConcreteIndices = mutableMapOf<String, MutableSet<String>>()
        flattenPaths.forEach { fieldPath ->
            if (conflictingPaths.contains(fieldPath.first)) {
                if (conflictingPathToConcreteIndices.containsKey(fieldPath.first)) {
                    val concreteIndexSet = conflictingPathToConcreteIndices[fieldPath.first]
                    concreteIndexSet!!.add(fieldPath.second)
                    conflictingPathToConcreteIndices[fieldPath.first] = concreteIndexSet
                } else {
                    val concreteIndexSet = mutableSetOf<String>()
                    concreteIndexSet.add(fieldPath.second)
                    conflictingPathToConcreteIndices[fieldPath.first] = concreteIndexSet
                }
            }
        }

        val newQueries = mutableListOf<DocLevelQuery>()
        val queriesById = mutableMapOf<String, Map<String, Any>>()
        queries.forEach {
            if (it.query.trimStart().startsWith("{")) {
                // Each backing index can have different scalar/object/multifield shapes.
                fieldParentsByIndex.forEach { (concreteIndex, parents) ->
                    val renamed = parents.mapValues { (field, parent) ->
                        val index = if (conflictingPaths.contains(parent)) concreteIndex else sourceIndex
                        "${parent}_${index}_$monitorId${field.removePrefix(parent).takeIf { field.startsWith(parent) } ?: ""}"
                    }
                    val newQuery = it.copy(id = "${it.id}_$concreteIndex")
                    newQueries.add(newQuery)
                    queriesById[newQuery.id] = queryForPercolation(it.query, renamed, it.fields)
                }
                return@forEach
            }
            val filteredConcreteIndices = mutableSetOf<String>()
            var query = it.query
            conflictingPaths.forEach { conflictingPath ->
                if (query.contains(conflictingPath)) {
                    val fields = fieldParentsByIndex.values.flatMap { it.entries }.filter { it.value == conflictingPath }
                        .associate { entry ->
                            val subfield = if (entry.key.startsWith("$conflictingPath.")) entry.key.removePrefix(conflictingPath) else ""
                            entry.key to "${conflictingPath}_<index>_$monitorId$subfield"
                        }
                    query = rewriteQueryString(query, fields)
                    filteredConcreteIndices.addAll(conflictingPathToConcreteIndices[conflictingPath]!!)
                }
            }

            if (filteredConcreteIndices.isNotEmpty()) {
                filteredConcreteIndices.forEach { filteredConcreteIndex ->
                    val newQuery = it.copy(
                        id = "${it.id}_$filteredConcreteIndex",
                        query = query.replace("<index>", filteredConcreteIndex)
                    )
                    newQueries.add(newQuery)
                }
            } else {
                newQueries.add(it.copy(id = "${it.id}_$sourceIndex"))
            }
        }

        newQueries.forEach {
            var query = it.query
            flattenPaths.forEach { fieldPath ->
                if (!conflictingPaths.contains(fieldPath.first)) {
                    val fields = fieldParentsByIndex.values.flatMap { it.entries }.filter { it.value == fieldPath.first }
                        .associate { entry ->
                            val subfield = if (entry.key.startsWith("${fieldPath.first}.")) entry.key.removePrefix(fieldPath.first) else ""
                            entry.key to "${fieldPath.first}_${sourceIndex}_$monitorId$subfield"
                        }
                    query = rewriteQueryString(query, fields)
                }
            }
            val indexRequest = IndexRequest(concreteQueryIndex)
                .id(it.id + "_$monitorId")
                .source(
                    mapOf(
                        "query" to (queriesById[it.id] ?: mapOf("query_string" to mapOf("query" to query, "fields" to it.fields))),
                        "monitor_id" to monitorId,
                        "index" to sourceIndex
                    )
                )
            indexRequests.add(indexRequest)
            log.debug("query $query added for execution of monitor $monitorId on index $sourceIndex")
        }
        log.debug("bulk inserting percolate [${queries.size}] queries")
        if (indexRequests.isNotEmpty()) {
            val bulkResponse: BulkResponse = client.suspendUntil {
                client.bulk(
                    BulkRequest().setRefreshPolicy(refreshPolicy).timeout(indexTimeout).add(indexRequests), it
                )
            }
            bulkResponse.forEach { bulkItemResponse ->
                if (bulkItemResponse.isFailed) {
                    log.error(bulkItemResponse.failureMessage)
                }
            }
        }
    }

    /**
     * Transforms the query if it includes an _exists_ clause to append the index name and the monitor id to the field value
     */
    private fun transformExistsQuery(query: String, conflictingPath: String, indexName: String, monitorId: String): String {
        return query
            .replace("_exists_: ", "_exists_:") // remove space to read exists query as one string
            .split("\\s+".toRegex())
            .joinToString(separator = " ") { segment ->
                if (segment.contains("_exists_:")) {
                    val trimSegement = segment.trim { it == '(' || it == ')' } // remove any delimiters from ends
                    val (_, value) = trimSegement.split(":", limit = 2) // split into key and value
                    val newString = if (value == conflictingPath)
                        segment.replace(conflictingPath, "${conflictingPath}_${indexName}_$monitorId") else segment
                    newString
                } else {
                    segment
                }
            }
    }

    internal open suspend fun updateQueryIndexMappings(
        monitor: Monitor,
        monitorMetadata: MonitorMetadata,
        sourceIndex: String,
        sourceIndexFieldLimit: Long,
        updatedProperties: MutableMap<String, Any>,
        indexTimeout: TimeValue,
        refreshPolicy: RefreshPolicy = RefreshPolicy.IMMEDIATE,
        isRetry: Boolean = false
    ): Pair<AcknowledgedResponse, String> {
        var targetQueryIndex = monitorMetadata.sourceToQueryIndexMapping[sourceIndex + monitor.id]
        if (
            targetQueryIndex == null || (
                targetQueryIndex == monitor.dataSources.queryIndex &&
                    monitor.deleteQueryIndexInEveryRun == true
                )
        ) {
            // Fix: old != caused churn — a concrete index always differs from the alias, so the branch
            // fired every run. == also catches the rare case where the alias name itself was persisted.
            targetQueryIndex = getWriteIndexNameForAlias(monitor.dataSources.queryIndex)
            if (targetQueryIndex == null) {
                val message = "Failed to get write index for queryIndex alias:${monitor.dataSources.queryIndex}"
                log.error(message)
                throw AlertingException.wrap(
                    OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR)
                )
            }
            monitorMetadata.sourceToQueryIndexMapping[sourceIndex + monitor.id] = targetQueryIndex
        }
        val updateMappingRequest = PutMappingRequest(targetQueryIndex)
        updateMappingRequest.source(mapOf<String, Any>("properties" to updatedProperties))
        var updateMappingResponse = AcknowledgedResponse(false)
        try {
            // Adjust max field limit in mappings for query index, if needed.
            adjustMaxFieldLimitForQueryIndex(sourceIndexFieldLimit, targetQueryIndex)
            updateMappingResponse = client.suspendUntil {
                client.admin().indices().putMapping(updateMappingRequest, it)
            }
            return Pair(updateMappingResponse, targetQueryIndex)
        } catch (e: Exception) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            log.warn(
                "PUT mapping failed on queryIndex $targetQueryIndex " +
                    "[${unwrappedException.javaClass.simpleName}]: ${unwrappedException.message}"
            )
            // If we reached limit for total number of fields in mappings, do a rollover here
            if (unwrappedException.message?.contains("Limit of total fields") == true) {
                try {
                    // Do queryIndex rollover
                    targetQueryIndex = rolloverQueryIndex(monitor)
                    // Adjust max field limit in mappings for new index.
                    adjustMaxFieldLimitForQueryIndex(sourceIndexFieldLimit, targetQueryIndex)
                    // PUT mappings to newly created index
                    val updateMappingRequest = PutMappingRequest(targetQueryIndex)
                    updateMappingRequest.source(mapOf<String, Any>("properties" to updatedProperties))
                    updateMappingResponse = client.suspendUntil {
                        client.admin().indices().putMapping(updateMappingRequest, it)
                    }
                } catch (e: Exception) {
                    // If we reached limit for total number of fields in mappings after rollover
                    // it means that source index has more then (FIELD_LIMIT - 3) fields (every query index has 3 fields defined)
                    // TODO maybe split queries/mappings between multiple query indices?
                    val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
                    log.debug("exception after rollover queryIndex index: $targetQueryIndex exception: ${unwrappedException.message}")
                    if (unwrappedException.message?.contains("Limit of total fields") == true) {
                        val errorMessage =
                            "Monitor [${monitorMetadata.monitorId}] can't process index [$sourceIndex] due to field mapping limit"
                        log.error(errorMessage)
                        throw AlertingException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR, e)
                    } else {
                        throw AlertingException.wrap(e)
                    }
                }
            } else {
                // retry with deleting query index (bounded to a single retry)
                if (monitor.deleteQueryIndexInEveryRun == true && !isRetry) {
                    try {
                        log.error(
                            "unknown exception during PUT mapping on queryIndex: $targetQueryIndex, " +
                                "retrying with deletion of query index",
                            e
                        )
                        if (docLevelQueryIndexExists(monitor.dataSources)) {
                            val ack = monitorCtx.docLevelMonitorQueries!!.deleteDocLevelQueryIndex(monitor.dataSources)
                            if (!ack) {
                                log.error(
                                    "Deletion of concrete queryIndex:${monitor.dataSources.queryIndex} is not ack'd! " +
                                        "for monitor ${monitor.id}"
                                )
                            }
                        }
                        initDocLevelQueryIndex(monitor.dataSources)
                        indexDocLevelQueries(
                            monitor = monitor,
                            monitorId = monitor.id,
                            monitorMetadata = monitorMetadata,
                            refreshPolicy = refreshPolicy,
                            indexTimeout = indexTimeout,
                            isRetry = true
                        )
                        val retriedQueryIndex =
                            monitorMetadata.sourceToQueryIndexMapping[sourceIndex + monitor.id] ?: targetQueryIndex
                        return Pair(AcknowledgedResponse(false), retriedQueryIndex)
                    } catch (e: Exception) {
                        log.error(
                            "Doc level monitor ${monitor.id}: unknown exception during " +
                                "PUT mapping on queryIndex: $targetQueryIndex",
                            e
                        )
                        val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
                        throw AlertingException.wrap(unwrappedException)
                    }
                } else {
                    log.error(
                        "Doc level monitor ${monitor.id}: unknown exception during " +
                            "PUT mapping on queryIndex: $targetQueryIndex",
                        e
                    )
                    throw AlertingException.wrap(unwrappedException)
                }
            }
        }
        // We did rollover, so try to apply mappings again on new targetQueryIndex
        if (targetQueryIndex.isNotEmpty()) {
            // add newly created index to monitor's metadata object so that we can fetch it later on, when either applying mappings or running queries
            monitorMetadata.sourceToQueryIndexMapping[sourceIndex + monitor.id] = targetQueryIndex
        } else {
            val failureMessage = "Failed to resolve targetQueryIndex!"
            log.error(failureMessage)
            throw AlertingException(failureMessage, RestStatus.INTERNAL_SERVER_ERROR, IllegalStateException(failureMessage))
        }
        return Pair(updateMappingResponse, targetQueryIndex)
    }

    /**
     * merge conflicting leaf fields in the mapping tree
     */
    private fun mergeConflictingFields(oldField: Map<String, Any>, newField: Map<String, Any>): Map<String, Any> {
        val mergedField = mutableMapOf<String, Any>()
        oldField.entries.forEach {
            if (newField.containsKey(it.key)) {
                if (it.value is Map<*, *> && newField[it.key] is Map<*, *>) {
                    mergedField[it.key] =
                        mergeConflictingFields(it.value as Map<String, Any>, newField[it.key] as Map<String, Any>)
                } else {
                    mergedField[it.key] = it.value
                }
            } else {
                mergedField[it.key] = it.value
            }
        }

        newField.entries.forEach {
            if (!oldField.containsKey(it.key)) {
                mergedField[it.key] = it.value
            }
        }
        return mergedField
    }

    /**
     * get all fields which have same name but different mappings belonging to an index pattern
     */
    @Suppress("UNCHECKED_CAST")
    fun getAllConflictingFields(clusterState: ClusterState, concreteIndices: List<String>): Set<String> {
        val indexPropertiesList = mutableListOf<Map<String, Any>>()
        concreteIndices.forEach { concreteIndexName ->
            if (clusterState.routingTable.hasIndex(concreteIndexName)) {
                val indexMetadata = clusterState.metadata.index(concreteIndexName)
                val properties = indexMetadata.mapping()?.sourceAsMap?.get("properties")
                if (properties is Map<*, *>) {
                    indexPropertiesList.add(properties as Map<String, Any>)
                }
            }
        }
        return findConflictingFields(indexPropertiesList)
    }

    /**
     * Finds all field paths whose mappings differ across [indexPropertiesList], including alias
     * definitions whose target path changed and aliases whose target field is itself conflicting.
     */
    fun findConflictingFields(indexPropertiesList: List<Map<String, Any>>): Set<String> {
        val conflictingFields = mutableSetOf<String>()
        val allFlattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
        val aliasToTargetPaths = mutableMapOf<String, MutableSet<String>>()
        val leafNodeProcessor =
            fun(fieldName: String, _: String, props: MutableMap<String, Any>): Triple<String, String, MutableMap<String, Any>> {
                return Triple(fieldName, fieldName, props)
            }

        indexPropertiesList.forEach { rawProperties ->
            // Deep-copy before traversal: traverseMappingsAndUpdate mutates the tree in-place
            // (leaf renaming). Without the copy, mutations corrupt cluster-state metadata.
            val properties = deepCopyMap(rawProperties)
            val flattenPaths = mutableMapOf<String, MutableMap<String, Any>>()
            traverseMappingsAndUpdate(properties, "", leafNodeProcessor, flattenPaths)

            flattenPaths.forEach {
                if (it.value["type"] == "alias") {
                    val targetPath = it.value["path"] as? String
                    if (targetPath != null) {
                        aliasToTargetPaths.getOrPut(it.key) { mutableSetOf() }.add(targetPath)
                    }
                }
                val existing = allFlattenPaths[it.key]
                if (existing != null && existing != it.value) {
                    conflictingFields.add(it.key)
                }
                allFlattenPaths.putIfAbsent(it.key, it.value)
            }
        }

        aliasToTargetPaths.forEach { (aliasField, targetPaths) ->
            if (targetPaths.any { conflictingFields.contains(it) }) {
                conflictingFields.add(aliasField)
            }
        }
        return conflictingFields
    }

    /**
     * checks the max field limit for a concrete index
     */
    private suspend fun checkMaxFieldLimit(sourceIndex: String): Long {
        val getSettingsResponse: GetSettingsResponse = client.suspendUntil {
            admin().indices().getSettings(GetSettingsRequest().indices(sourceIndex), it)
        }
        return getSettingsResponse.getSetting(sourceIndex, INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key)?.toLong() ?: 1000L
    }

    /**
     * Adjusts max field limit index setting for query index if source index has higher limit.
     * This will prevent max field limit exception, when source index has more fields then query index limit
     */
    internal open suspend fun adjustMaxFieldLimitForQueryIndex(sourceIndexFieldLimit: Long, concreteQueryIndex: String) {
        val getSettingsResponse: GetSettingsResponse = client.suspendUntil {
            admin().indices().getSettings(GetSettingsRequest().indices(concreteQueryIndex), it)
        }
        val queryIndexLimit =
            getSettingsResponse.getSetting(concreteQueryIndex, INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key)?.toLong() ?: 1000L
        // Our query index initially has 3 fields we defined and 5 more builtin metadata fields in mappings so we have to account for that
        if (sourceIndexFieldLimit > (queryIndexLimit - QUERY_INDEX_BASE_FIELDS_COUNT)) {
            val updateSettingsResponse: AcknowledgedResponse = client.suspendUntil {
                admin().indices().updateSettings(
                    UpdateSettingsRequest(concreteQueryIndex).settings(
                        Settings.builder().put(
                            INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.key,
                            sourceIndexFieldLimit + QUERY_INDEX_BASE_FIELDS_COUNT
                        )
                    ),
                    it
                )
            }
            if (!updateSettingsResponse.isAcknowledged) {
                log.warn(
                    "Field mapping limit settings update was not acknowledged for query index [$concreteQueryIndex]. " +
                        "The index may still have the old limit of $queryIndexLimit."
                )
            }
        }
    }

    internal open suspend fun rolloverQueryIndex(monitor: Monitor): String {
        val queryIndex = monitor.dataSources.queryIndex
        val queryIndexPattern = monitor.dataSources.queryIndex + INDEX_PATTERN_SUFFIX

        val request = RolloverRequest(queryIndex, null)
        request.createIndexRequest.index(queryIndexPattern)
            .mapping(docLevelQueriesMappings())
            .settings(docLevelQueriesSettings())
        val response: RolloverResponse = client.suspendUntil {
            client.admin().indices().rolloverIndex(request, it)
        }
        if (response.isRolledOver == false) {
            val message = "failed to rollover queryIndex:$queryIndex queryIndexPattern:$queryIndexPattern"
            log.error(message)
            throw AlertingException.wrap(
                OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR)
            )
        }
        return response.newIndex
    }

    /**
     * Returns the concrete write-index name for [alias], or `null` if the alias or write index doesn't exist.
     * Always returns a concrete index name, never an alias name.
     */
    internal open fun getWriteIndexNameForAlias(alias: String): String? {
        return this.clusterService.state().metadata().indicesLookup?.get(alias)?.writeIndex?.index?.name
    }
}
