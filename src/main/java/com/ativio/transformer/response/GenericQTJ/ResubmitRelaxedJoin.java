package com.ativio.transformer.response.GenericQTJ;

import com.attivio.sdk.esb.PlatformMessage;
import com.attivio.sdk.search.QueryLanguages;
import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.QueryResponse;
import com.attivio.sdk.search.query.AccessControlQuery;
import com.attivio.sdk.search.query.BooleanAndQuery;
import com.attivio.sdk.search.query.BooleanOrQuery;
import com.attivio.sdk.search.query.BoostQuery;
import com.attivio.sdk.search.query.CompositeJoinQuery;
import com.attivio.sdk.search.query.JoinQuery;
import com.attivio.sdk.search.query.Query;
import com.attivio.sdk.search.query.QueryString;
import com.attivio.sdk.search.query.SubQuery;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ConfigurationOption.OptionLevel;
import com.attivio.sdk.server.component.routing.MessageRoutingComponent;

@ConfigurationOptionInfo(displayName = "Relax Join Query", description = "Resubmit a join query, switching from AND to OR", groups = {
		@ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.PLATFORM_COMPONENT, propertyNames = {
				"resubmitWorkflow" }), })
public class ResubmitRelaxedJoin implements MessageRoutingComponent {

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
		if (!(message instanceof QueryResponse)) {
			return null;
		}
		QueryResponse response = (QueryResponse) message;
		QueryRequest request = response.getQueryRequest();
		boolean shouldResubmitQuery = response.getDocuments().size() < 1
				&& request.getResubmits() < request.getMaxResubmits();
		if (shouldResubmitQuery) {

			BooleanOrQuery orQuery = this.generateBooleanOrQuery(request);
			if (orQuery != null) {
				request.incrementResubmits();
				request.setQuery(orQuery.toString(), QueryLanguages.ADVANCED);
				return resubmitWorkflow;
			}
		}
		return null;
	}

	private BooleanOrQuery generateBooleanOrQuery(QueryRequest qr) {
		BooleanOrQuery orQuery = new BooleanOrQuery();
		Query originalQuery = qr.getQuery();
		if (originalQuery instanceof AccessControlQuery) {
			originalQuery = ((AccessControlQuery) originalQuery).getQuery();
		}
		if (originalQuery instanceof BoostQuery) {
			originalQuery = ((BoostQuery) originalQuery).getQuery();
		}
		if (originalQuery instanceof CompositeJoinQuery) {
			CompositeJoinQuery joinQuery = (CompositeJoinQuery) originalQuery;
			originalQuery = joinQuery.getQuery();
		}
		if (originalQuery instanceof JoinQuery) {
			JoinQuery join = (JoinQuery) originalQuery;
			originalQuery = join.getQuery();
		}
		if (originalQuery instanceof AccessControlQuery) {
			originalQuery = ((AccessControlQuery) originalQuery).getQuery();
		}
		if (originalQuery instanceof BoostQuery) {
			originalQuery = ((BoostQuery) originalQuery).getQuery();
		}
		if (originalQuery instanceof BooleanAndQuery) {
			BooleanAndQuery andQuery = (BooleanAndQuery) originalQuery;
			for (Query clause : andQuery) {
				orQuery.add(clause);
			}
			return orQuery;
		} else if (originalQuery instanceof SubQuery) {
			SubQuery sq = (SubQuery) originalQuery;
			if (sq.getQuery() instanceof BooleanAndQuery) {
				BooleanAndQuery andQuery = (BooleanAndQuery) sq.getQuery();
				for (Query clause : andQuery) {
					orQuery.add(clause);
				}
				return orQuery;
			} else if (sq.getQuery() instanceof BooleanOrQuery) {
				return orQuery;
			}
		} else if (originalQuery instanceof QueryString) {
			QueryString qs = (QueryString) originalQuery;
			String queryString = qs.getQueryString();
			if (!queryString.contains("\"")) {
				String[] terms = queryString.split(" ");
				for (String t : terms) {
					orQuery.add(t);
				}
			}
			return orQuery;
		}
		return null;
	}

}
