use crate::{backend::Schema, net::parameter::ParameterValue};

#[derive(Debug)]
pub(crate) struct SearchPath<'a> {
    search_path: &'a [String],
    user: &'a str,
}

impl<'a> SearchPath<'a> {
    /// Return a list of schemas the tables the user can see.
    pub(crate) fn resolve(&'a self) -> Vec<&'a str> {
        let mut schemas = vec![];

        for path in self.search_path {
            match path.as_str() {
                "$user" => schemas.push(self.user),
                path => schemas.push(path),
            }
        }

        schemas
    }

    pub(crate) fn new(
        user: &'a str,
        search_path: Option<&'a ParameterValue>,
        schema: &'a Schema,
    ) -> Self {
        let search_path = search_path.map_or(schema.search_path(), ParameterValue::as_slice);
        Self { search_path, user }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_search_path() {
        let param = vec!["$user".into(), "public".into()];
        let user = "pgdog";
        let resolver = SearchPath {
            search_path: &param,
            user,
        };
        let res = resolver.resolve();
        assert_eq!(res, vec!["pgdog", "public"]);
    }

    #[test]
    fn test_search_path_single_schema_override() {
        let schema = Schema::from_parts(vec!["$user".into(), "public".into()], HashMap::new());
        let param = ParameterValue::String("tenant_b".into());
        let resolver = SearchPath::new("pgdog", Some(&param), &schema);
        assert_eq!(resolver.resolve(), vec!["tenant_b"]);
    }

    #[test]
    fn test_search_path_multi_schema_override() {
        let schema = Schema::from_parts(vec!["public".into()], HashMap::new());
        let param = ParameterValue::Tuple(vec!["$user".into(), "tenant_b".into()]);
        let resolver = SearchPath::new("pgdog", Some(&param), &schema);
        assert_eq!(resolver.resolve(), vec!["pgdog", "tenant_b"]);
    }
}
