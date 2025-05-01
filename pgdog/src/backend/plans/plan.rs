use super::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    #[serde(rename = "Plan")]
    plan: PlanNode,
}

impl QueryPlan {
    pub fn from_json(plan: &str) -> Result<Self, Error> {
        let value: serde_json::Value = serde_json::from_str(plan)?;
        if let Some(list) = value.as_array() {
            Ok(serde_json::from_value(list[0].clone())?)
        } else {
            Err(Error::Deser)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanNode {
    #[serde(rename = "Node Type")]
    pub node_type: String,
    #[serde(rename = "Startup Cost")]
    pub startup_cost: f64,
    #[serde(rename = "Total Cost")]
    pub total_cost: f64,
    #[serde(rename = "Plan Rows")]
    pub plan_rows: i64,
    #[serde(rename = "Plan Width")]
    pub plan_width: i64,
    #[serde(rename = "Plans", default = "Vec::new")]
    pub plans: Vec<PlanNode>,
}

#[cfg(test)]
mod test {
    use super::QueryPlan;

    #[test]
    fn test_deser() {
        let original = r#"
            [
              {
                "Plan": {
                  "Node Type": "Hash Join",
                  "Parallel Aware": false,
                  "Async Capable": false,
                  "Join Type": "Inner",
                  "Startup Cost": 37.0,
                  "Total Cost": 66.83,
                  "Plan Rows": 1200,
                  "Plan Width": 56,
                  "Inner Unique": true,
                  "Hash Cond": "(orders.id = users.id)",
                  "Plans": [
                    {
                      "Node Type": "Seq Scan",
                      "Parent Relationship": "Outer",
                      "Parallel Aware": false,
                      "Async Capable": false,
                      "Relation Name": "orders",
                      "Alias": "orders",
                      "Startup Cost": 0.0,
                      "Total Cost": 25.7,
                      "Plan Rows": 1570,
                      "Plan Width": 24
                    },
                    {
                      "Node Type": "Hash",
                      "Parent Relationship": "Inner",
                      "Parallel Aware": false,
                      "Async Capable": false,
                      "Startup Cost": 22.0,
                      "Total Cost": 22.0,
                      "Plan Rows": 1200,
                      "Plan Width": 40,
                      "Plans": [
                        {
                          "Node Type": "Seq Scan",
                          "Parent Relationship": "Outer",
                          "Parallel Aware": false,
                          "Async Capable": false,
                          "Relation Name": "users",
                          "Alias": "users",
                          "Startup Cost": 0.0,
                          "Total Cost": 22.0,
                          "Plan Rows": 1200,
                          "Plan Width": 40
                        }
                      ]
                    }
                  ]
                }
              }
            ]"#;
        let de: QueryPlan = QueryPlan::from_json(original).unwrap();
        assert_eq!(de.plan.startup_cost, 37.0);
        assert_eq!(de.plan.total_cost, 66.83);
    }
}
