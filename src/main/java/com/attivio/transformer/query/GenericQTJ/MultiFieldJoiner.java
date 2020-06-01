package com.attivio.transformer.query.GenericQTJ;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.search.QueryFeedback;
import com.attivio.sdk.search.QueryRequest;
import com.attivio.sdk.search.query.BooleanAndQuery;
import com.attivio.sdk.search.query.BooleanOrQuery;
import com.attivio.sdk.search.query.JoinClause;
import com.attivio.sdk.search.query.JoinMode;
import com.attivio.sdk.search.query.JoinQuery;
import com.attivio.sdk.search.query.PhraseQuery;
import com.attivio.sdk.search.query.Query;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.annotation.ConfigurationOption.OptionLevel;

@ConfigurationOptionInfo(displayName = "Multi-Field Table Joiner", description = "Transforms query into the equivalent of a Composite Join, but with the ability to specify different join fields for each metadata table", groups = {
		@ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.PLATFORM_COMPONENT, propertyNames = {
				"joinFields" }),
		@ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.ADVANCED, propertyNames = {
				"mimickComposite" }) })
public class MultiFieldJoiner extends GenericCompositeJoiner {

	private Map<String, String> joinFields = new HashMap<String, String>();
	private boolean mimickComposite = false;

	private Logger log = LoggerFactory.getLogger(this.getClass());

	@ConfigurationOption(displayName = "Default Join Field", description = "The default Field to join on (must be the same field across both sources)", optionLevel = OptionLevel.Required)
	@Override
	public String getJoinField() {
		return joinField;
	}

	@ConfigurationOption(displayName = "Child Table Join Fields", description = "Map of table names to the fields they should be joined on, if a field other than the default join field should be used", formEntryClass = ConfigurationOption.STRING_TO_STRING_MAP)
	public Map<String, String> getJoinFields() {
		return joinFields;
	}

	public void setJoinFields(Map<String, String> joinFields) {
		this.joinFields = joinFields;
	}

	@ConfigurationOption(displayName = "Composite Mode", description = "Should the search term be searched for in all child tables as well, similar to the behavior of the CompositeJoin. WARNING: Consider performance impacts before turning this on", formEntryClass = ConfigurationOption.FALSE_SWITCH_VALUE)
	public boolean isMimickComposite() {
		return mimickComposite;
	}

	public void setMimickComposite(boolean mimickComposite) {
		this.mimickComposite = mimickComposite;
	}

	@Override
	protected Query buildCompositeJoinQuery(Map<String, List<Query>> facetFiltersMap, QueryRequest qr,
			List<QueryFeedback> feedback) throws AttivioException {

		Query completeQuery;
		Query userQuery = qr.getQuery();
		Query initialJoinQuery = this.generateInitialJoin(userQuery, facetFiltersMap, feedback);
		if (this.provideFeedback) {
			String message = "Adding initial Join Query : " + initialJoinQuery;
			feedback.add(new QueryFeedback(this.getClass().getName(), "MultiFieldJoiner", message));
			log.trace(message);
		}
		if (this.mimickComposite) {
			BooleanOrQuery orWrapperQuery = new BooleanOrQuery();
			orWrapperQuery.add(initialJoinQuery);
			for (String table : this.childTables.keySet()) {
				Query childTableJoinQuery = this.generateMetadataJoinQuery(userQuery, table, facetFiltersMap, feedback);
				if (this.provideFeedback) {
					String message = "Adding additional Join Query : " + childTableJoinQuery;
					feedback.add(new QueryFeedback(this.getClass().getName(), "MultiFieldJoiner", message));
					log.trace(message);
				}
				orWrapperQuery.add(childTableJoinQuery);
			}
			completeQuery = orWrapperQuery;
		} else {
			completeQuery = initialJoinQuery;
		}
		if (this.provideFeedback) {
			String message = "Setting final Query : " + completeQuery;
			feedback.add(new QueryFeedback(this.getClass().getName(), "MultiFieldJoiner", message));
			log.trace(message);
		}
		return completeQuery;
	}

	/**
	 * Generates a JoinQuery that handles the scenario where the match on the user
	 * query in in the primary table, so all metadata tables just need to be plain
	 * Outer Join Clauses
	 * 
	 * @param userQuery
	 *            The original query from the user
	 * @param facetFiltersMap
	 *            Facet Queries that should be added to the join clauses
	 * @param feedback
	 * @return The {@code JoinQuery}
	 * @throws AttivioException
	 */
	private JoinQuery generateInitialJoin(Query userQuery, Map<String, List<Query>> facetFiltersMap,
			List<QueryFeedback> feedback) throws AttivioException {
		BooleanAndQuery andQuery = new BooleanAndQuery(userQuery);
		andQuery.add(super.generateFromQuery());
		JoinQuery join = new JoinQuery(andQuery);
		for (String childTable : this.childTables.keySet()) {
			JoinMode jm = JoinMode.fromExternal(this.childTables.get(childTable));
			join.add(this.generateGenericClause(childTable, this.joinField, jm, this.joinFields, facetFiltersMap));
		}
		return join;
	}

	/**
	 * Generates a JoinQuery representing a Join of primary tables to metadata
	 * tables, where the match on the original user query is in one of the metadata
	 * tables
	 * 
	 * @param userQuery
	 *            The original query from the user
	 * @param primaryTables
	 *            The primary data table that metadata should be joined to
	 * @param metadataQueryTable
	 * @param metadataTables
	 *            List of metadata tables that should be joined to the primary
	 *            documents
	 * @param joinField
	 *            The default field to join metadata fields on
	 * @param joinFields
	 *            Map of table to field for when the defaul join field shouldn't be
	 *            used
	 * @param facetFiltersMap
	 *            Facet Queries that should be added to the join clauses
	 * @param feedback
	 * @return A {@code JoinQuery}
	 * @throws AttivioException
	 */
	private JoinQuery generateMetadataJoinQuery(Query userQuery, String metadataQueryTable,
			Map<String, List<Query>> facetFiltersMap, List<QueryFeedback> feedback) throws AttivioException {
		Query primaryTableQuery = super.generateFromQuery();
		JoinQuery joinQuery = new JoinQuery(primaryTableQuery);
		BooleanAndQuery andQuery = new BooleanAndQuery(new PhraseQuery(this.collectionFieldName, metadataQueryTable));
		andQuery.add(userQuery);
		if (facetFiltersMap.containsKey(metadataQueryTable)) {
			andQuery.add(facetFiltersMap.get(metadataQueryTable));
		}
		String joinKeyField = joinFields.containsKey(metadataQueryTable) ? joinFields.get(metadataQueryTable)
				: this.joinField;
		joinQuery.add(new JoinClause(andQuery, JoinMode.INNER, joinKeyField, joinKeyField));
		for (String childTable : this.childTables.keySet()) {
			if (childTable.equals(metadataQueryTable)) {
				continue;
			}
			JoinMode jm = JoinMode.fromExternal(this.childTables.get(childTable));
			joinQuery.add(this.generateGenericClause(childTable, joinKeyField, jm, joinFields, facetFiltersMap));
		}
		return joinQuery;
	}

	/**
	 * Utility function to generate the Join Clauses for metadata tables that don't
	 * need to be searched for the a user query
	 * 
	 * @param table
	 *            The name of the table the clause is for
	 * @param joinField
	 *            The default field to join on
	 * @param joinFields
	 *            Map of table names to field names for when the default join field
	 *            should not be used
	 * @param facetFiltersMap
	 *            Facet filter queries to apply to the clause's query
	 * @return the {@code JoinClause} for the specified table, with facet filter
	 *         queries applied
	 */
	private JoinClause generateGenericClause(String table, String joinField, JoinMode joinMode,
			Map<String, String> joinFields, Map<String, List<Query>> facetFiltersMap) {
		String field = joinField;
		if (joinFields.containsKey(table)) {
			field = joinFields.get(table);
		}
		PhraseQuery tableQuery = new PhraseQuery(this.collectionFieldName, table);
		Query clauseQuery = tableQuery;
		if (facetFiltersMap.containsKey(table)) {
			BooleanAndQuery andQuery = new BooleanAndQuery(tableQuery);
			for (Query facetQuery : facetFiltersMap.get(table)) {
				andQuery.add(facetQuery);
			}
			clauseQuery = andQuery;
		}
		JoinClause clause = new JoinClause(clauseQuery, joinMode, field, field);
		if (this.maxChildDocs!=null && this.maxChildDocs.get(table)!=null) {
		  clause.setRollupLimit(this.maxChildDocs.get(table));
		}
		return clause;
	}

}
