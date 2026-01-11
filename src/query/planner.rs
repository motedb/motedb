//! Query planner and optimizer

use crate::Result;

/// Operator types in execution plan
#[derive(Debug, Clone)]
pub enum Operator {
    /// Full table scan
    Scan { source: String },
    /// Filter operation
    Filter { predicate: String },
    /// Projection operation
    Project { columns: Vec<String> },
    /// Index scan
    IndexScan { index_type: String, range: String },
    /// Join operation
    Join { left: Box<ExecutionPlan>, right: Box<ExecutionPlan> },
}

/// Execution plan tree
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Root operator
    pub operator: Operator,
    /// Child plans
    pub children: Vec<ExecutionPlan>,
    /// Estimated cost
    pub cost: f64,
}

impl ExecutionPlan {
    /// Create a simple scan plan
    pub fn scan(source: String) -> Self {
        Self {
            operator: Operator::Scan { source },
            children: Vec::new(),
            cost: 100.0,
        }
    }
    
    /// Create an index scan plan
    pub fn index_scan(index_type: String, range: String) -> Self {
        Self {
            operator: Operator::IndexScan { index_type, range },
            children: Vec::new(),
            cost: 10.0, // Index scan is cheaper
        }
    }
    
    /// Add a filter operator
    pub fn with_filter(self, predicate: String) -> Self {
        let filter_plan = Self {
            operator: Operator::Filter { predicate },
            children: vec![self.clone()],
            cost: self.cost + 5.0,
        };
        filter_plan
    }
    
    /// Add a projection operator
    pub fn with_project(self, columns: Vec<String>) -> Self {
        let project_plan = Self {
            operator: Operator::Project { columns },
            children: vec![self.clone()],
            cost: self.cost + 1.0,
        };
        project_plan
    }
}

/// Query planner - converts SQL/API queries to execution plans
pub struct QueryPlanner {
    /// Enable query optimization
    enable_optimization: bool,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Result<Self> {
        Ok(Self {
            enable_optimization: true,
        })
    }
    
    /// Create with optimization disabled (for testing)
    pub fn without_optimization() -> Result<Self> {
        Ok(Self {
            enable_optimization: false,
        })
    }
    
    /// Plan a simple range query
    pub fn plan_range_query(&self, index_type: &str, start: i64, end: i64) -> Result<ExecutionPlan> {
        let range = format!("[{}, {})", start, end);
        
        if self.enable_optimization && self.should_use_index(index_type) {
            // Use index scan
            Ok(ExecutionPlan::index_scan(index_type.to_string(), range))
        } else {
            // Fallback to full scan with filter
            Ok(ExecutionPlan::scan("default".to_string())
                .with_filter(format!("timestamp BETWEEN {} AND {}", start, end)))
        }
    }
    
    /// Determine if index should be used
    fn should_use_index(&self, _index_type: &str) -> bool {
        // Future: Consider selectivity, cost model, statistics
        true
    }
    
    /// Optimize an execution plan
    pub fn optimize(&self, plan: ExecutionPlan) -> Result<ExecutionPlan> {
        if !self.enable_optimization {
            return Ok(plan);
        }
        
        // Future optimizations:
        // - Predicate pushdown
        // - Column pruning
        // - Index selection
        // - Join reordering
        
        Ok(plan)
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new().unwrap()
    }
}
