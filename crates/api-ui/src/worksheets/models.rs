use chrono::{DateTime, Utc};
use core_history::WorksheetId;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use utoipa::{IntoParams, ToSchema};

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

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum SortOrder {
    Ascending,
    #[default]
    Descending,
}

impl Display for SortOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match *self {
            Self::Ascending => "ascending",
            Self::Descending => "descending",
        };
        write!(f, "{s}")
    }
}

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum SortBy {
    Name,
    #[default]
    CreatedAt,
    UpdatedAt,
}

impl Display for SortBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match *self {
            Self::Name => "name",
            Self::CreatedAt => "createdAt",
            Self::UpdatedAt => "updatedAt",
        };
        write!(f, "{s}")
    }
}

// Use inline attribute as a workaround, for https://github.com/juhaku/utoipa/issues/1284
// as otherwise it doesn't create schema for enum variants in Query

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Eq, ToSchema, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(default, rename_all = "camelCase")]
pub struct GetWorksheetsParams {
    /// Sort order
    #[param(inline)]
    pub sort_order: Option<SortOrder>,
    /// Sort by
    #[param(inline)]
    pub sort_by: Option<SortBy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorksheetCreateResponse {
    #[serde(flatten)]
    pub data: Worksheet,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorksheetResponse {
    #[serde(flatten)]
    pub data: Worksheet,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorksheetsResponse {
    pub items: Vec<Worksheet>,
}
