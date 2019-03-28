package com.attivio.transformer.query.GenericQTJ;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.search.QueryFeedback;
import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.facet.FacetFilter;
import com.attivio.sdk.search.query.BooleanAndQuery;
import com.attivio.sdk.search.query.BooleanOrQuery;
import com.attivio.sdk.search.query.CompositeJoinQuery;
import com.attivio.sdk.search.query.FacetQuery;
import com.attivio.sdk.search.query.JoinMode;
import com.attivio.sdk.search.query.Query;
import com.attivio.util.ObjectUtils;

public class GenericCompositeJoinerTest {

	private GenericCompositeJoiner setup() {
		GenericCompositeJoiner joiner = new GenericCompositeJoiner();
		joiner.setJoinField("metadataLink");
		joiner.setMaxChildDocs("-1");
		joiner.setPrimaryTables(ObjectUtils.newList("dataTable"));
		List<String> tables = new ArrayList<String>();
		tables.add("metadata");
		tables.add("anotherMetadata Table");
		joiner.setMetadataTables(tables);
		List<String> propertiesToPreserve = new ArrayList<String>();
		propertiesToPreserve.add("testProperty");
		propertiesToPreserve.add("join.facet");
		joiner.setPropertiesToPreserve(propertiesToPreserve);
		joiner.setProvideFeedback(true);
		Map<String, String> metadataFacetFields = new HashMap<String, String>();
		metadataFacetFields.put("metadata", "topic, company, transaction_amount, date");
		metadataFacetFields.put("anotherMetadata Table", "people");
		joiner.setMetadataFacetFields(metadataFacetFields);
		return joiner;
	}

	@Test
	public void testNoFacetFilters() {
		QueryRequest qr = new QueryRequest();
		qr.setQuery("content:electronic", "SIMPLE");
		qr.setProperty("testProperty", true);

		GenericCompositeJoiner joiner = this.setup();
		try {
			List<QueryFeedback> feedback = joiner.processQuery(qr);

			assertTrue(qr.hasProperty("testProperty"));
			assertTrue(Boolean.valueOf(qr.getProperty("testProperty").toString()));

			System.out.println(qr.getQuery().prettyFormat());
			for (QueryFeedback feedbackItem : feedback) {
				System.out.println(feedbackItem);
			}

			assertTrue(qr.getQuery() instanceof CompositeJoinQuery);

			CompositeJoinQuery join = (CompositeJoinQuery) qr.getQuery();

			Query fromQuery = join.getFromQuery();
			assertTrue(fromQuery instanceof BooleanOrQuery);
			assertEquals("OR(table:dataTable)", fromQuery.toString());

			assertTrue(join.getClauses().get(0).getQuery() instanceof BooleanOrQuery);
			assertEquals(join.getClauses().get(0).getMode(), JoinMode.OUTER);

			assertTrue(join.getClauses().get(1).getQuery() instanceof BooleanOrQuery);
			assertEquals(join.getClauses().get(1).getMode(), JoinMode.OUTER);
		} catch (AttivioException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testWithFacetFilter() {
		QueryRequest qr = new QueryRequest();
		qr.setQuery("content:electronic", "SIMPLE");
		qr.setProperty("testProperty", true);
		FacetFilter filter1 = new FacetFilter();
		FacetFilter filter2 = new FacetFilter();
		FacetQuery fq1 = new FacetQuery("topic", "management");
		FacetQuery fq2 = new FacetQuery("people", "Joe Smith");
		filter1.setFilter(fq1);
		filter2.setFilter(fq2);
		System.out.println("Adding facet query: " + fq1);
		qr.addFacetFilter(filter1);
		qr.addFacetFilter(filter2);

		GenericCompositeJoiner joiner = this.setup();
		try {
			List<QueryFeedback> feedback = joiner.processQuery(qr);

			assertTrue(qr.hasProperty("testProperty"));
			assertTrue(Boolean.valueOf(qr.getProperty("testProperty").toString()));

			System.out.println(qr.getQuery().prettyFormat());
			for (QueryFeedback feedbackItem : feedback) {
				System.out.println(feedbackItem);
			}

			assertTrue(qr.getQuery() instanceof CompositeJoinQuery);

			CompositeJoinQuery join = (CompositeJoinQuery) qr.getQuery();

			Query fromQuery = join.getFromQuery();
			assertTrue(fromQuery instanceof BooleanOrQuery);
			assertEquals("OR(table:dataTable)", fromQuery.toString());

			assertTrue(join.getClauses().get(0).getQuery() instanceof BooleanAndQuery);
			assertEquals(join.getClauses().get(0).getMode(), JoinMode.INNER);

			assertTrue(join.getClauses().get(1).getQuery() instanceof BooleanAndQuery);
			assertEquals(join.getClauses().get(1).getMode(), JoinMode.INNER);
		} catch (AttivioException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testWithRangeFacetFilter() {
		QueryRequest qr = new QueryRequest();
		qr.setQuery("content:electronic", "SIMPLE");
		qr.setProperty("testProperty", true);
		FacetFilter filter1 = new FacetFilter();
		FacetFilter filter2 = new FacetFilter();
		FacetFilter filter3 = new FacetFilter();
		FacetQuery fq1 = new FacetQuery("transaction_amount", "RANGE(10000, 12000, upper=exclusive)");
		FacetQuery fq2 = new FacetQuery("people", "Joe Smith");
		FacetQuery fq3 = new FacetQuery("date", "RANGE(2007-01-01T00:00:00, 2007-01-01T00:00:00, upper=inclusive)");
		filter1.setFilter(fq1);
		filter2.setFilter(fq2);
		filter3.setFilter(fq3);
		System.out.println("Adding facet query: " + fq1);
		System.out.println("Adding facet query: " + fq2);
		System.out.println("Adding facet query: " + fq3);
		qr.addFacetFilter(filter1);
		qr.addFacetFilter(filter2);
		qr.addFacetFilter(filter3);
		GenericCompositeJoiner joiner = this.setup();
		try {
			List<QueryFeedback> feedback = joiner.processQuery(qr);

			assertTrue(qr.hasProperty("testProperty"));
			assertTrue(Boolean.valueOf(qr.getProperty("testProperty").toString()));

			System.out.println(qr.getQuery().prettyFormat());
			for (QueryFeedback feedbackItem : feedback) {
				System.out.println(feedbackItem);
			}

			assertTrue(qr.getQuery() instanceof CompositeJoinQuery);

			CompositeJoinQuery join = (CompositeJoinQuery) qr.getQuery();

			Query fromQuery = join.getFromQuery();
			assertTrue(fromQuery instanceof BooleanOrQuery);
			assertEquals("OR(table:dataTable)", fromQuery.toString());

			assertTrue(join.getClauses().get(0).getQuery() instanceof BooleanAndQuery);
			assertEquals(join.getClauses().get(0).getMode(), JoinMode.INNER);

			assertTrue(join.getClauses().get(1).getQuery() instanceof BooleanAndQuery);
			assertEquals(join.getClauses().get(1).getMode(), JoinMode.INNER);
		} catch (AttivioException e) {
			e.printStackTrace();
		}
	}

}