// DEMO ONLY: model-level metadata, not real business records or verified warehouse mappings.
// Re-running is idempotent for this demo namespace. Existing unrelated nodes are untouched.
UNWIND [
  {id:'user', name:'用户', kind:'object', model:'dim_users', grain:'一行一个用户', keys:['user_id'], columns:['user_id','user_name','birth_date']},
  {id:'shop', name:'店铺', kind:'object', model:'dim_shops', grain:'一行一个店铺', keys:['shop_id'], columns:['shop_id','shop_name']},
  {id:'product', name:'商品', kind:'object', model:'dim_products', grain:'一行一个商品', keys:['product_id'], columns:['product_id','product_name','category']},
  {id:'sku', name:'SKU', kind:'object', model:'dim_skus', grain:'一行一个SKU', keys:['sku_id'], columns:['sku_id','product_id','sku_name']},
  {id:'order', name:'订单', kind:'object', model:'fct_orders', grain:'一行一张订单', keys:['order_id'], columns:['order_id','user_id','shop_id','ordered_at','paid_at','status','paid_amount']},
  {id:'order_item', name:'订单明细', kind:'object', model:'fct_order_items', grain:'一行一个订单明细', keys:['order_item_id'], columns:['order_item_id','order_id','sku_id','quantity','paid_amount']},
  {id:'inventory', name:'库存历史', kind:'state', model:'fct_inventory_history', grain:'一行一个SKU在一个店铺的有效期状态', keys:['sku_id','shop_id','valid_from'], columns:['sku_id','shop_id','available_qty','valid_from','valid_to']},
  {id:'purchase_order', name:'采购单', kind:'object', model:'fct_purchase_orders', grain:'一行一张采购单', keys:['purchase_order_id'], columns:['purchase_order_id','supplier_id','ordered_at','expected_at']},
  {id:'purchase_item', name:'采购明细', kind:'object', model:'fct_purchase_items', grain:'一行一个采购明细', keys:['purchase_item_id'], columns:['purchase_item_id','purchase_order_id','sku_id','shop_id','ordered_qty']},
  {id:'supplier', name:'供应商', kind:'object', model:'dim_suppliers', grain:'一行一个供应商', keys:['supplier_id'], columns:['supplier_id','supplier_name']},
  {id:'receipt', name:'到货事件', kind:'event', model:'fct_receipts', grain:'一行一次采购明细到货事件，允许分批到货', keys:['receipt_id'], columns:['receipt_id','purchase_item_id','received_at','received_qty']},
  {id:'review', name:'商品评价', kind:'event', model:'fct_reviews', grain:'一行一条评价', keys:['review_id'], columns:['review_id','user_id','product_id','rating','reviewed_at']}
] AS item
MERGE (n:OntologyDemo:BusinessType {demo_id:'commerce_metadata_demo_20261026', id:item.id})
SET n.name=item.name, n.kind=item.kind, n.grain=item.grain, n.identity=item.keys,
    n.description='演示业务类型，不是具体业务实例', n.is_demo=true, n.schema_version=1
MERGE (m:OntologyDemo:DbtModel {demo_id:'commerce_metadata_demo_20261026', id:item.model})
SET m.name=item.model, m.columns=item.columns, m.database='demo_warehouse', m.schema='analytics_demo',
    m.alias=item.model, m.materialization='table', m.mapping_verified=false,
    m.description='示例物理映射，未连接或验证真实数仓', m.is_demo=true
MERGE (n)-[r:MAPS_TO_MODEL {demo_id:'commerce_metadata_demo_20261026'}]->(m)
RETURN count(n) AS business_types;

UNWIND [
  {s:'order',t:'user',p:'placed_by',name:'由用户下单',sf:['user_id'],tf:['user_id'],card:'many_to_one'},
  {s:'order',t:'shop',p:'placed_at',name:'归属店铺',sf:['shop_id'],tf:['shop_id'],card:'many_to_one'},
  {s:'order_item',t:'order',p:'belongs_to_order',name:'属于订单',sf:['order_id'],tf:['order_id'],card:'many_to_one'},
  {s:'order_item',t:'sku',p:'purchases',name:'购买SKU',sf:['sku_id'],tf:['sku_id'],card:'many_to_one'},
  {s:'sku',t:'product',p:'variant_of',name:'商品规格',sf:['product_id'],tf:['product_id'],card:'many_to_one'},
  {s:'inventory',t:'sku',p:'stock_of',name:'记录SKU库存',sf:['sku_id'],tf:['sku_id'],card:'many_to_one'},
  {s:'inventory',t:'shop',p:'stock_at',name:'记录店铺库存',sf:['shop_id'],tf:['shop_id'],card:'many_to_one'},
  {s:'purchase_item',t:'sku',p:'procures',name:'采购SKU',sf:['sku_id'],tf:['sku_id'],card:'many_to_one'},
  {s:'purchase_item',t:'shop',p:'delivers_to',name:'送往店铺',sf:['shop_id'],tf:['shop_id'],card:'many_to_one'},
  {s:'purchase_item',t:'purchase_order',p:'belongs_to_purchase',name:'属于采购单',sf:['purchase_order_id'],tf:['purchase_order_id'],card:'many_to_one'},
  {s:'purchase_order',t:'supplier',p:'ordered_from',name:'向供应商采购',sf:['supplier_id'],tf:['supplier_id'],card:'many_to_one'},
  {s:'receipt',t:'purchase_item',p:'receipt_for',name:'采购明细到货',sf:['purchase_item_id'],tf:['purchase_item_id'],card:'many_to_one'},
  {s:'review',t:'user',p:'written_by',name:'用户发表评价',sf:['user_id'],tf:['user_id'],card:'many_to_one'},
  {s:'review',t:'product',p:'evaluates',name:'评价商品',sf:['product_id'],tf:['product_id'],card:'many_to_one'}
] AS item
MATCH (s:OntologyDemo:BusinessType {demo_id:'commerce_metadata_demo_20261026',id:item.s})
MATCH (t:OntologyDemo:BusinessType {demo_id:'commerce_metadata_demo_20261026',id:item.t})
MERGE (s)-[r:BUSINESS_RELATION {demo_id:'commerce_metadata_demo_20261026',id:item.p}]->(t)
SET r.name=item.name, r.predicate=item.p, r.source_fields=item.sf, r.target_fields=item.tf,
    r.cardinality=item.card, r.validated=false,
    r.description='示例连接路径；不是因果关系；反向遍历时需注意一对多聚合膨胀'
RETURN count(r) AS business_relations;

MATCH (o:OntologyDemo:BusinessType {demo_id:'commerce_metadata_demo_20261026',id:'order'})
MERGE (m:OntologyDemo:Metric {demo_id:'commerce_metadata_demo_20261026',id:'gmv'})
SET m.name='GMV（示意）', m.metricflow_metric='demo_gmv', m.verified=false,
    m.description='指标占位，不含正式口径，未注册到MetricFlow', m.is_demo=true
MERGE (m)-[:MEASURES {demo_id:'commerce_metadata_demo_20261026'}]->(o)
RETURN m.name AS metric;

MATCH (n:OntologyDemo:BusinessType {demo_id:'commerce_metadata_demo_20261026',id:'inventory'})
SET n.valid_from='valid_from', n.valid_to='valid_to', n.interval='[valid_from,valid_to)',
    n.open_end='valid_to IS NULL', n.timezone='Asia/Shanghai',
    n.analysis_note='按SKU、店铺和分析时点匹配历史有效区间，禁止用当前库存推断上月缺货'
RETURN n.name AS history_type;

MATCH (n:OntologyDemo:BusinessType {demo_id:'commerce_metadata_demo_20261026',id:'receipt'})
SET n.event_time='received_at', n.timezone='Asia/Shanghai',
    n.analysis_note='沿采购明细和采购单定位供应商，比较实际到货与承诺时间；部分到货需结合数量'
RETURN n.name AS event_type;
