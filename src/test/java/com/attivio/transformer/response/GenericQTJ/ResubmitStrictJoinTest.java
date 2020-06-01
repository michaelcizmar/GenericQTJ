package com.attivio.transformer.response.GenericQTJ;

import static org.junit.Assert.*;

import org.junit.Test;

import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.QueryResponse;
import com.attivio.sdk.search.SearchDocument;
import com.attivio.sdk.search.SearchDocumentList;
import com.attivio.transformer.query.GenericQTJ.GenericCompositeJoiner;

public class ResubmitStrictJoinTest {

	private static String WORKFLOW = "myCustomSearchWorkflow";

	@Test
	public void testShouldResubmit() {
		ResubmitStrictJoin resubmitter = new ResubmitStrictJoin();
		resubmitter.setResubmitWorkflow(WORKFLOW);

		QueryRequest request = new QueryRequest("*:*");
		request.setResubmits(0);
		request.setMaxResubmits(1);
		request.setProperty(GenericCompositeJoiner.STRICT_QUERY_PROPERTY_NAME, true);
		QueryResponse response = new QueryResponse(request);
		response.setDocuments(new SearchDocumentList());

		String newWorkflow = resubmitter.getRoutingKey(response);
		assertNotNull(newWorkflow);
		assertEquals(WORKFLOW, newWorkflow);
	}

	@Test
	public void testSufficientResults() {
		ResubmitStrictJoin resubmitter = new ResubmitStrictJoin();
		resubmitter.setResubmitWorkflow(WORKFLOW);

		QueryRequest request = new QueryRequest("*:*");
		request.setResubmits(0);
		request.setMaxResubmits(1);
		request.setProperty(GenericCompositeJoiner.STRICT_QUERY_PROPERTY_NAME, true);
		QueryResponse response = new QueryResponse(request);
		SearchDocumentList docList = new SearchDocumentList();
		docList.add(new SearchDocument("doc1"));
		response.setDocuments(docList);

		String newWorkflow = resubmitter.getRoutingKey(response);
		assertNull(newWorkflow);
	}

	@Test
	public void testNoResubmitMissingProperty() {
		ResubmitStrictJoin resubmitter = new ResubmitStrictJoin();
		resubmitter.setResubmitWorkflow(WORKFLOW);

		QueryRequest request = new QueryRequest("*:*");
		request.setResubmits(0);
		request.setMaxResubmits(1);
		QueryResponse response = new QueryResponse(request);
		response.setDocuments(new SearchDocumentList());

		String newWorkflow = resubmitter.getRoutingKey(response);
		assertNull(newWorkflow);
	}

	@Test
	public void testNoResubmitMaxResubmitsHit() {
		ResubmitStrictJoin resubmitter = new ResubmitStrictJoin();
		resubmitter.setResubmitWorkflow(WORKFLOW);

		QueryRequest request = new QueryRequest("*:*");
		request.setProperty(GenericCompositeJoiner.STRICT_QUERY_PROPERTY_NAME, true);
		request.setResubmits(1);
		request.setMaxResubmits(1);
		QueryResponse response = new QueryResponse(request);
		response.setDocuments(new SearchDocumentList());

		String newWorkflow = resubmitter.getRoutingKey(response);
		assertNull(newWorkflow);
	}
}
