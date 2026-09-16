/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.alerting.resthandler

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.AlertingPlugin
import org.opensearch.alerting.util.context
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.GetMonitorRequest
import org.opensearch.commons.alerting.action.GetMonitorResponse
import org.opensearch.commons.alerting.action.GetWorkflowRequest
import org.opensearch.commons.alerting.action.GetWorkflowResponse
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.BaseRestHandler.RestChannelConsumer
import org.opensearch.rest.RestHandler.ReplacedRoute
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.RestRequest
import org.opensearch.rest.RestRequest.Method.GET
import org.opensearch.rest.RestRequest.Method.HEAD
import org.opensearch.rest.action.RestActions
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.search.fetch.subphase.FetchSourceContext
import org.opensearch.transport.client.node.NodeClient

private val log = LogManager.getLogger(RestGetMonitorAction::class.java)

/**
 * This class consists of the REST handler to retrieve a monitor .
 */
class RestGetMonitorAction : BaseRestHandler() {

    override fun getName(): String {
        return "get_monitor_action"
    }

    override fun routes(): List<Route> {
        return listOf()
    }

    override fun replacedRoutes(): MutableList<ReplacedRoute> {
        return mutableListOf(
            // Get a specific monitor
            ReplacedRoute(
                GET,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}",
                GET,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}"
            ),
            ReplacedRoute(
                HEAD,
                "${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}",
                HEAD,
                "${AlertingPlugin.LEGACY_OPENDISTRO_MONITOR_BASE_URI}/{monitorID}"
            )
        )
    }

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        log.debug("${request.method()} ${AlertingPlugin.MONITOR_BASE_URI}/{monitorID}")

        val monitorId = request.param("monitorID")
        if (monitorId == null || monitorId.isEmpty()) {
            throw IllegalArgumentException("missing id")
        }

        var srcContext = context(request)
        if (request.method() == HEAD) {
            srcContext = FetchSourceContext.DO_NOT_FETCH_SOURCE
        }
        val getMonitorRequest = GetMonitorRequest(monitorId, RestActions.parseVersion(request), request.method(), srcContext)
        return RestChannelConsumer { channel ->
            client.execute(
                AlertingActions.GET_MONITOR_ACTION_TYPE,
                getMonitorRequest,
                object : ActionListener<GetMonitorResponse> {
                    override fun onResponse(response: GetMonitorResponse) {
                        RestToXContentListener<GetMonitorResponse>(channel).onResponse(response)
                    }

                    override fun onFailure(e: Exception) {
                        val status = (e as? AlertingException)?.status()
                            ?: (e.cause as? org.opensearch.OpenSearchStatusException)?.status()
                        if (status == RestStatus.NOT_FOUND) {
                            // Document exists as a Workflow; fetch it and embed under "monitor" key
                            // so the Dashboards backend (which checks for that key) can render it
                            val getWorkflowRequest = GetWorkflowRequest(monitorId, request.method())
                            client.execute(
                                AlertingActions.GET_WORKFLOW_ACTION_TYPE,
                                getWorkflowRequest,
                                object : ActionListener<GetWorkflowResponse> {
                                    override fun onResponse(wfResp: GetWorkflowResponse) {
                                        try {
                                            val builder = channel.newBuilder()
                                            builder.startObject()
                                            builder.field("_id", wfResp.id)
                                            builder.field("_version", wfResp.version)
                                            builder.field("_seq_no", wfResp.seqNo)
                                            builder.field("_primary_term", wfResp.primaryTerm)
                                            wfResp.workflow?.let { builder.field("monitor", it) }
                                            builder.endObject()
                                            channel.sendResponse(BytesRestResponse(RestStatus.OK, builder))
                                        } catch (ex: Exception) {
                                            RestToXContentListener<GetMonitorResponse>(channel).onFailure(ex)
                                        }
                                    }
                                    override fun onFailure(ex: Exception) {
                                        RestToXContentListener<GetMonitorResponse>(channel).onFailure(ex)
                                    }
                                }
                            )
                        } else {
                            RestToXContentListener<GetMonitorResponse>(channel).onFailure(e)
                        }
                    }
                }
            )
        }
    }
}
