use chrono::{DateTime, Utc};
use core_history::WorksheetId;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorksheetCreatePayload {
    pub name: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorksheetUpdatePayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Worksheet {
    pub id: WorksheetId,
    pub name: String,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<core_history::Worksheet> for Worksheet {
    fn from(worksheet: core_history::Worksheet) -> Self {
        Self {
            id: worksheet.id,
            name: worksheet.name,
            content: worksheet.content,
            created_at: worksheet.created_at,
            updated_at: worksheet.updated_at,
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<core_history::Worksheet> for Worksheet {
    fn into(self) -> core_history::Worksheet {
        core_history::Worksheet {
            id: self.id,
            name: self.name,
            content: self.content,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorksheetCreateResponse(pub Worksheet);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorksheetResponse(pub Worksheet);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorksheetsResponse {
    pub items: Vec<Worksheet>,
}
