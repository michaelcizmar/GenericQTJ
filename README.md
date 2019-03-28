# GenericQTJ
Suite of Query Transformers for Executing Joins

### What's In This eModule
There are two query transformers: the Generic Composite Joiner, and the Multi-Field Metadata Joiner. These components can be used to transform a user's query into a CompositeJoin, and to create a Join for multiple tables with different Join Keys, respectively. 

### Generic Composite Joiner
The Generic Composite Joiner transforms the query into a CompositeJoin Query. It can handle joins across multiple tables, and can also handle use cases where facet filtering is based off of fields on the child documents. 

![Generic Composite Joiner](https://github.com/attivio/GenericQTJ/blob/master/screenshots/generic_composite_joiner_config_1.PNG)

| Configuration Property | Description |
| --- | --- |
| Primary Tables | The tables that get used in the FROM clause of the join. If there are multiple tables listed here, they get put into an OR query. <br> **Note:** Either this property or Non-Primary Tables must be provided. |
| Non-Primary Tables | Used in place of the Primary Tables property when instead of listing all tables to include in the FROM clause, you wish to just exclude some tables. For example, if you have wo metadata tables to treat as child documents, list them in the Non-Primary Tables property, and the FROM clause will become NOT(OR(table:non_primary_table_1, table:non_primary_table_2)). **Note:** Either this property or Primary Tables must be provided. |  |
| Metadata Tables | The tables that get used for child documents |
| Join Field | The field to join the tables on. All tables must join on the same field, as this is required by the CompositeJoin syntax. |
| Max Child Docs | The maximum number of records to join to a parent record. Set to -1 for all records. |
| Properties to Preserve | Properties to copy over to the new Query |
| Metadata Facet Fields | Map of table names to a comma separated list of fields from that table that will be used for facets. <br> Child table facets need to be configured because of the way the query changed for facet filters from child records vs. parent records (flipping OUTER clauses to INNER clauses, how facet counts are aggregfated, etc. |
| Facet Metadata Tables | List of tables used for child records whose fields should be used in facet calculations. |
| Ignore Advanced Queries (Advanced Tab) | If set to *true*, advanced queries will not be modified by this transformer.  |
| Provide Query Feedback (Advanced Tab) | If set to *true*, detailed feedback will be provided (useful for troubleshooting) |

### Multi-Field Joiner
The Multi-Field Joiner transforms the query into a JOIN query (regular JOIN, not a CompositeJoin). It can handle joins across multiple tables, even when the tables need to be joined on different fields, and can also handle use cases where facet filtering is based off of fields on the child documents. 

![Multi-Field Joiner](https://github.com/attivio/GenericQTJ/blob/master/screenshots/multifield_joiner_config_1.PNG)

The Multi-Field Joiner extends the Generic Composite Joiner, so the configuration is almost exactly the same. The only difference is that the Multi-Field Joiner has an additional property for mapping table names to the field name to use for the join key. 

| Configuration Property | Description |
| --- | --- |
| Metadata Table Join Fields | Map of table names to the fields that they will be joined on. |

