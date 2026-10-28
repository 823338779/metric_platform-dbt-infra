// 1. Full metadata graph: types, mappings and metric placeholder.
MATCH (n:OntologyDemo {demo_id:'commerce_metadata_demo_20261026'})
OPTIONAL MATCH (n)-[r]->(m:OntologyDemo {demo_id:'commerce_metadata_demo_20261026'})
RETURN n,r,m;

// 2. Business-only view, hiding the physical model nodes.
MATCH (n:OntologyDemo {demo_id:'commerce_metadata_demo_20261026'})
WHERE n:BusinessType OR n:Metric
OPTIONAL MATCH (n)-[r:BUSINESS_RELATION|MEASURES]->(m:OntologyDemo {demo_id:'commerce_metadata_demo_20261026'})
RETURN n,r,m;

// 3. Investigative path from GMV to supplier, not a causal assertion.
MATCH p=(g:OntologyDemo:Metric {demo_id:'commerce_metadata_demo_20261026',id:'gmv'})
  -[:MEASURES]->(:BusinessType {demo_id:'commerce_metadata_demo_20261026',id:'order'})
  <-[:BUSINESS_RELATION]-(:BusinessType {demo_id:'commerce_metadata_demo_20261026',id:'order_item'})
  -[:BUSINESS_RELATION]->(:BusinessType {demo_id:'commerce_metadata_demo_20261026',id:'sku'})
  <-[:BUSINESS_RELATION]-(:BusinessType {demo_id:'commerce_metadata_demo_20261026',id:'purchase_item'})
  -[:BUSINESS_RELATION]->(:BusinessType {demo_id:'commerce_metadata_demo_20261026',id:'purchase_order'})
  -[:BUSINESS_RELATION]->(:BusinessType {demo_id:'commerce_metadata_demo_20261026',id:'supplier'})
RETURN p;

// 4. Read join metadata for a prospective investigation.
MATCH (a:OntologyDemo:BusinessType {demo_id:'commerce_metadata_demo_20261026'})
  -[r:BUSINESS_RELATION]->(b:OntologyDemo:BusinessType {demo_id:'commerce_metadata_demo_20261026'})
RETURN a.name AS source,r.name AS relationship,b.name AS target,
       r.source_fields AS source_fields,r.target_fields AS target_fields,r.cardinality AS cardinality;
