package com.attivio.transformer.query.GenericQTJ;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.attivio.sdk.AttivioException;
import com.attivio.sdk.schema.FieldNames;
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

@ConfigurationOptionInfo(displayName = "Multi-Field Metadata Joiner", description = "Transforms query into the equivalend of a Composite Join, but with the ability to specify different join fields for each metadata table", groups = {
		@ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.PLATFORM_COMPONENT, propertyNames = {
				"joinFields" }), })
public class MultiFieldJoiner extends GenericCompositeJoiner {

	private Map<String, String> joinFields = new HashMap<String, String>();

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

	@Override
	protected Query buildCompositeJoinQuery(Map<String, List<Query>> facetFiltersMap, QueryRequest qr,
			List<QueryFeedback> feedback) throws AttivioException {

		BooleanOrQuery fullQuery = new BooleanOrQuery();
		Query userQuery = qr.getQuery();
		Query initialJoinQuery = this.generateInitialJoin(userQuery, this.primaryTables, this.childTables,
				this.joinField, this.joinFields, facetFiltersMap, feedback);
		if (this.provideFeedback) {
			feedback.add(new QueryFeedback(this.getClass().getName(), "MultiFieldJoiner",
					"Adding initial Join Query : " + initialJoinQuery));
		}
		fullQuery.add(initialJoinQuery);
		for (String table : this.childTables.keySet()) {
			Query metadataJoinQuery = this.generateMetadataJoinQuery(userQuery, this.primaryTables, table,
					this.joinField, this.joinFields, facetFiltersMap, feedback);
			if (this.provideFeedback) {
				feedback.add(new QueryFeedback(this.getClass().getName(), "MultiFieldJoiner",
						"Adding additional Join Query : " + metadataJoinQuery));
			}
			fullQuery.add(metadataJoinQuery);
		}
		if (this.provideFeedback) {
			feedback.add(new QueryFeedback(this.getClass().getName(), "MultiFieldJoiner",
					"Setting final Query : " + fullQuery));
		}
		return fullQuery;
	}

	/**
	 * Generates a JoinQuery that handles the scenario where the match on the user
	 * query in in the primary table, so all metadata tables just need to be plain
	 * Outer Join Clauses
	 * 
	 * @param userQuery
	 *            The original query from the user
	 * @param primaryTables
	 *            The primary data table that metadata should be joined to
	 * @param childTables
	 *            Map of metadata tables that should be joined to the primary
	 *            documents to the join mode that should be used for the clause for
	 *            that table
	 * @param joinField
	 *            The defaul field to join metadata fields on
	 * @param joinFields
	 *            Map of table to field for when the defaul join field shouldn't be
	 *            used
	 * @param facetFiltersMap
	 *            Facet Queries that should be added to the join clauses
	 * @param feedback
	 * @return The {@code JoinQuery}
	 * @throws AttivioException
	 */
	private JoinQuery generateInitialJoin(Query userQuery, List<String> primaryTables, Map<String, String> childTables,
			String joinField, Map<String, String> joinFields, Map<String, List<Query>> facetFiltersMap,
			List<QueryFeedback> feedback) throws AttivioException {
		BooleanAndQuery andQuery = new BooleanAndQuery(userQuery);
		andQuery.add(super.generateFromQuery());
		JoinQuery join = new JoinQuery(andQuery);
		for (String childTable : childTables.keySet()) {
			JoinMode jm = JoinMode.fromExternal(childTables.get(childTable));
			join.add(this.generateGenericClause(childTable, joinField, jm, joinFields, facetFiltersMap));
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
	 *            The defaul field to join metadata fields on
	 * @param joinFields
	 *            Map of table to field for when the defaul join field shouldn't be
	 *            used
	 * @param facetFiltersMap
	 *            Facet Queries that should be added to the join clauses
	 * @param feedback
	 * @return A {@code JoinQuery}
	 * @throws AttivioException
	 */
	private JoinQuery generateMetadataJoinQuery(Query userQuery, List<String> primaryTables, String metadataQueryTable,
			String joinField, Map<String, String> joinFields, Map<String, List<Query>> facetFiltersMap,
			List<QueryFeedback> feedback) throws AttivioException {
		Query primaryTableQuery = super.generateFromQuery();
		JoinQuery joinQuery = new JoinQuery(primaryTableQuery);
		BooleanAndQuery andQuery = new BooleanAndQuery(new PhraseQuery(FieldNames.TABLE, metadataQueryTable));
		andQuery.add(userQuery);
		if (facetFiltersMap.containsKey(metadataQueryTable)) {
			andQuery.add(facetFiltersMap.get(metadataQueryTable));
		}
		if (joinFields.containsKey(metadataQueryTable)) {
			joinField = joinFields.get(metadataQueryTable);
		}
		joinQuery.add(new JoinClause(andQuery, JoinMode.INNER, joinField, joinField));
		for (String childTable : this.childTables.keySet()) {
			if (childTable.equals(metadataQueryTable)) {
				continue;
			}
			JoinMode jm = JoinMode.fromExternal(this.childTables.get(childTable));
			joinQuery.add(this.generateGenericClause(childTable, joinField, jm, joinFields, facetFiltersMap));
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
		PhraseQuery tableQuery = new PhraseQuery(FieldNames.TABLE, table);
		Query clauseQuery = tableQuery;
		if (facetFiltersMap.containsKey(table)) {
			BooleanAndQuery andQuery = new BooleanAndQuery(tableQuery);
			for (Query facetQuery : facetFiltersMap.get(table)) {
				andQuery.add(facetQuery);
			}
			clauseQuery = andQuery;
		}
		JoinClause clause = new JoinClause(clauseQuery, joinMode, field, field);
		return clause;
	}

}
