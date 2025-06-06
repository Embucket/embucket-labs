pub mod functions_checker;
pub mod functions_list;
mod generated_snowflake_functions;

/// Information about a Snowflake function
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    /// Function name
    pub name: &'static str,
    /// Function description
    pub description: &'static str,
    /// Link to Snowflake documentation
    pub documentation_url: Option<&'static str>,
    /// GitHub issue link (to be filled later)
    pub issue_url: Option<&'static str>,
    /// Subcategory if applicable
    pub subcategory: Option<&'static str>,
}

impl FunctionInfo {
    #[must_use]
    pub const fn new(name: &'static str, description: &'static str) -> Self {
        Self {
            name,
            description,
            documentation_url: None,
            issue_url: None,
            subcategory: None,
        }
    }

    #[must_use]
    pub const fn with_docs(mut self, url: &'static str) -> Self {
        self.documentation_url = Some(url);
        self
    }

    #[must_use]
    pub const fn with_subcategory(mut self, subcategory: &'static str) -> Self {
        self.subcategory = Some(subcategory);
        self
    }

    /// Add issue URL to existing function info (for later updates)
    #[must_use]
    pub const fn with_issue_url(mut self, issue_url: &'static str) -> Self {
        self.issue_url = Some(issue_url);
        self
    }

    /// Get the preferred URL for details (issue URL if available, otherwise documentation URL)
    #[must_use]
    pub fn get_preferred_url(&self) -> Option<&str> {
        self.issue_url.or(self.documentation_url)
    }
}
