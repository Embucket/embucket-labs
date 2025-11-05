
#[derive(Debug, Clone)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub enum OrderBy {
    Name(OrderDirection),
    ParentName(OrderDirection),
    CreatedAt(OrderDirection),
    UpdatedAt(OrderDirection),
}

#[derive(Debug, Clone)]
pub struct ListParams {
    pub id: Option<i64>,
    pub parent_id: Option<i64>,
    pub parent_name: Option<String>,
    pub offset: Option<i64>,
    pub limit: Option<i64>,
    pub search: Option<String>,
    pub order_by: Vec<OrderBy>,
}

impl Default for ListParams {
    fn default() -> Self {
        Self {
            id: None,
            parent_id: None,
            parent_name: None,
            offset: None,
            limit: None,
            search: None,
            order_by: vec![OrderBy::CreatedAt(OrderDirection::Desc)],
        }
    }
}

impl ListParams {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_id(self, id: i64) -> Self {
        Self {
            id: Some(id),
            ..self
        }
    }
    pub fn with_parent_id(self, parent_id: i64) -> Self {
        Self {
            parent_id: Some(parent_id),
            ..self
        }
    }
    pub fn with_parent_name(self, parent_name: String) -> Self {
        Self {
            parent_name: Some(parent_name),
            ..self
        }
    }
    pub fn with_offset(self, offset: i64) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }
    pub fn with_limit(self, limit: i64) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }
    pub fn with_search(self, search: String) -> Self {
        Self {
            search: Some(search),
            ..self
        }
    }
    pub fn with_order_by(self, order_by: Vec<OrderBy>) -> Self {
        Self {
            order_by,
            ..self
        }
    }
}