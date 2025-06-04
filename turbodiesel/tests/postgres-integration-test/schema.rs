// @generated automatically by Diesel CLI.

diesel::table! {
    students (id) {
        id -> Int4,
        name -> Text,
        dob -> Nullable<Date>,
    }
}
