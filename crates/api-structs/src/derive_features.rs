#[macro_export]
macro_rules! with_derives {
    ($item:item) => {
        #[cfg_attr(feature = "schema", derive(ToSchema))]
        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "partial", derive(PartialEq))]
        #[cfg_attr(feature = "eq", derive(Eq))]
        #[cfg_attr(feature = "serde", serde(rename_all = "camelCase"))]
        $item
    };
}
