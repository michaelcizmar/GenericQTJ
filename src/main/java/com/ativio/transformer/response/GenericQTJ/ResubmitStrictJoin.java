package com.ativio.transformer.response.GenericQTJ;

import com.attivio.sdk.esb.PlatformMessage;
import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.QueryResponse;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ConfigurationOption.OptionLevel;
import com.attivio.sdk.server.component.routing.MessageRoutingComponent;
import com.attivio.transformer.query.GenericQTJ.GenericCompositeJoiner;

@ConfigurationOptionInfo(displayName = "Resubmit Strict Join Query", description = "Resubmit a join query that was a strict join", groups = {
		@ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.PLATFORM_COMPONENT, propertyNames = {
				"resubmitWorkflow" }), })
public class ResubmitStrictJoin implements MessageRoutingComponent {

	private String resubmitWorkflow;

	@ConfigurationOption(displayName = "Resubmit Workflow", description = "Get the name of the workflow to re-submit queries to.", optionLevel = OptionLevel.Required)
	public String getResubmitWorkflow() {
		return resubmitWorkflow;
	}

	public void setResubmitWorkflow(String resubmitWorkflow) {
		this.resubmitWorkflow = resubmitWorkflow;
	}

	@Override
	public String getRoutingKey(PlatformMessage message) {
		// At this point in the workflow, the PlatformMessage we get should be a
		// QueryResponse.
		if (!(message instanceof QueryResponse)) {
			return null;
		}
		QueryResponse response = (QueryResponse) message;
		QueryRequest request = response.getQueryRequest();

		if (request.getResubmits() < request.getMaxResubmits()) {
			boolean shouldResubmitQuery = false;
			shouldResubmitQuery = this.processQuery(response, request);
			if (shouldResubmitQuery) {
				request.incrementResubmits();
				return resubmitWorkflow;
			}
		}
		return null;
	}

	boolean processQuery(QueryResponse response, QueryRequest request) {
		// No need to resubmit if there are hits found
		if (response.getDocuments().size() < 1) {
			// If no hits found, let's see if the strict join was used. No need to resubmit
			// if it wasn't.
			if (request.hasProperty(GenericCompositeJoiner.STRICT_QUERY_PROPERTY_NAME)
					&& request.getProperty(GenericCompositeJoiner.STRICT_QUERY_PROPERTY_NAME, false)) {
				return true;
			}
		}
		return false;
	}
}
