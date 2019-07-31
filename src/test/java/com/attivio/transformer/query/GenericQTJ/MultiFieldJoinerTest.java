package com.attivio.transformer.query.GenericQTJ;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.search.QueryFeedback;
import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.facet.FacetFilter;
import com.attivio.sdk.search.query.FacetQuery;
import com.attivio.util.ObjectUtils;

public class MultiFieldJoinerTest {

	private MultiFieldJoiner setup() {
		MultiFieldJoiner joiner = new MultiFieldJoiner();
		joiner.setMaxChildDocs(ObjectUtils.newMap("metadata", new Integer(5)));
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
		metadataFacetFields.put("metadata", "topic, company");
		metadataFacetFields.put("anotherMetadata Table", "people");
		joiner.setChildTableFacetFields(metadataFacetFields);

		Map<String, String> overridingJoinFields = new HashMap<String, String>();
		overridingJoinFields.put("anotherMetadata Table", "uniqueJoinField");
		joiner.setJoinFields(overridingJoinFields);
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
		} catch (AttivioException e) {
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
		} catch (AttivioException e) {
			e.printStackTrace();
		}
	}

}