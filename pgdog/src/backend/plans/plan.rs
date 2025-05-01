use super::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    #[serde(rename = "Plan")]
    plan: PlanNode,
    #[serde(skip, default)]
    estimate: Estimate,
}

impl QueryPlan {
    pub(crate) fn from_json(plan: &str) -> Result<Self, Error> {
        let value: serde_json::Value = serde_json::from_str(plan)?;
        if let Some(list) = value.as_array() {
            let plan: QueryPlan = serde_json::from_value(list.get(0).ok_or(Error::Deser)?.clone())?;
            Ok(plan.calculate())
        } else {
            Err(Error::Deser)
        }
    }

    fn calculate(mut self) -> Self {
        let mut stack = Vec::new();
        let mut estimate = Estimate::default();
        stack.push(self.plan.clone());

        while let Some(plan) = stack.pop() {
            match plan.node_type.as_str() {
                "Seq Scan" => {
                    estimate.plans.push(EstimateUnit::SeqScan {
                        rows: plan.plan_rows,
                        width: plan.plan_width,
                    });
                }
                "Index Scan" => {
                    estimate.plans.push(EstimateUnit::IndexScan {
                        rows: plan.plan_rows,
                        width: plan.plan_width,
                    });
                }

                _ => (),
            }

            for plan in plan.plans {
                stack.push(plan);
            }
        }
        self.estimate = estimate;
        self
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
    #[serde(rename = "Plans", default)]
    pub plans: Vec<PlanNode>,
}

#[derive(Debug, Clone, Default)]
pub struct Estimate {
    plans: Vec<EstimateUnit>,
}

impl Estimate {
    pub(crate) fn calculate(&self) -> i64 {
        self.plans
            .iter()
            .map(|unit| match unit {
                EstimateUnit::SeqScan { rows, width } => *rows * 2 * *width,
                EstimateUnit::IndexScan { rows, width } => *rows * *width,
            })
            .sum::<i64>()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EstimateUnit {
    SeqScan { rows: i64, width: i64 },
    IndexScan { rows: i64, width: i64 },
}

#[cfg(test)]
mod test {

    use super::*;

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
        assert_eq!(
            de.estimate.plans[1],
            EstimateUnit::SeqScan {
                rows: 1570,
                width: 24
            }
        );
    }
}
