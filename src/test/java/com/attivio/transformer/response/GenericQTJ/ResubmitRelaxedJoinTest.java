package com.attivio.transformer.response.GenericQTJ;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import com.attivio.sdk.AttivioException;
import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.QueryResponse;
import com.attivio.sdk.search.query.BooleanOrQuery;
import com.attivio.sdk.search.query.PhraseQuery;
import com.attivio.sdk.search.query.Query;
import com.attivio.transformer.query.GenericQTJ.GenericCompositeJoiner;
import com.attivio.util.ObjectUtils;

public class ResubmitRelaxedJoinTest {

	private GenericCompositeJoiner setup() {
		GenericCompositeJoiner joiner = new GenericCompositeJoiner();
		joiner.setJoinField("metadataLink");
		Map<String, Integer> maxDocs = new HashMap<String, Integer>();
		maxDocs.put("metadata", 5);
		joiner.setMaxChildDocs(maxDocs);
		joiner.setPrimaryTables(ObjectUtils.newList("dataTable"));
		Map<String, String> tables = new HashMap<String, String>();
		tables.put("metadata", "INNER");
		tables.put("anotherMetadata Table", "OUTER");
		joiner.setChildTables(tables);
		joiner.setProvideFeedback(true);
		Map<String, String> metadataFacetFields = new HashMap<String, String>();
		metadataFacetFields.put("metadata", "topic, company, transaction_amount, date");
		metadataFacetFields.put("anotherMetadata Table", "people");
		joiner.setChildTableFacetFields(metadataFacetFields);
		Map<String, Integer> boosts = new HashMap<String, Integer>();
		boosts.put("metadata", 5);
		joiner.setTableBoosts(boosts);
		joiner.setStrictChildMatching(false);
		return joiner;
	}

	@Test
	public void test() {
		QueryRequest qr = new QueryRequest();
		qr.setQuery("content:electronic", "SIMPLE");
		qr.setProperty("testProperty", true);
		qr.setMaxResubmits(1);

		GenericCompositeJoiner joiner = this.setup();
		try {
			joiner.processQuery(qr);

			QueryResponse response = new QueryResponse(qr);
			ResubmitRelaxedJoin resubmitter = new ResubmitRelaxedJoin();
			resubmitter.setResubmitWorkflow("customSearch");
			String resubmitWorkflow = resubmitter.getRoutingKey(response);
			assertEquals("customSearch", resubmitWorkflow);
			//assertTrue(response.getQueryRequest().getQuery() instanceof BooleanOrQuery);
			Query orQuery = (Query) response.getQueryRequest().getQuery();
			String clauses = orQuery.getQueryString();
			clauses.equalsIgnoreCase("OR(\"content:electronic\")\", qlang=advanced)");
			//todo: compare the query generated to the phrase query.  Looks like a phrase query.
			//assertEquals(1, clauses.length);
			//assertEquals(clauses[0], new PhraseQuery("content:electronic"));
		} catch (AttivioException e) {
			e.printStackTrace();
		}
	}
}
