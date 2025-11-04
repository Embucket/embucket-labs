// @generated automatically by Diesel CLI.

diesel::table! {
    databases (id) {
        id -> BigInt,
        ident -> Text,
        properties -> Nullable<Text>,
        volume_id -> BigInt,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    schemas (id) {
        id -> BigInt,
        ident -> Text,
        database_id -> BigInt,
        properties -> Nullable<Text>,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    tables (id) {
        id -> BigInt,
        ident -> Text,
        metadata -> Text,
        metadata_location -> Text,
        properties -> Text,
        volume_ident -> Nullable<Text>,
        volume_location -> Nullable<Text>,
        is_temporary -> Bool,
        format -> Text,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    volumes (id) {
        id -> BigInt,
        ident -> Text,
        volume_type -> Text,
        volume -> Text,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::joinable!(databases -> volumes (volume_id));
diesel::joinable!(schemas -> databases (database_id));

diesel::allow_tables_to_appear_in_same_query!(databases, schemas, tables, volumes,);
