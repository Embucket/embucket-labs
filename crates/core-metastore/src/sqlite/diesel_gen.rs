// @generated automatically by Diesel CLI.
diesel::table! {
    databases (id) {
        id -> Text,
        ident -> Text,
        properties -> Nullable<Text>,
        volume_ident -> Text,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    schemas (id) {
        id -> Text,
        ident -> Text,
        properties -> Nullable<Text>,
        created_at -> Text,
        updated_at -> Text,
    }
}

diesel::table! {
    tables (id) {
        id -> Text,
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
        id -> Text,
        ident -> Text,
        volume -> Text,
        created_at -> Text,
        updated_at -> Text,
    }
}
