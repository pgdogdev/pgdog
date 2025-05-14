use crate::{
    backend::Schema,
    net::{parameter::ParameterValue, Parameters},
};

#[derive(Debug)]
pub struct SearchPath<'a> {
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

    pub(crate) fn new(user: &'a str, params: &'a Parameters, schema: &'a Schema) -> Self {
        let default_path = schema.search_path();
        let search_path = match params.get("search_path") {
            Some(ParameterValue::Tuple(overriden)) => overriden.as_slice(),
            _ => default_path,
        };
        Self { search_path, user }
    }
}

#[cfg(test)]
mod test {

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
}
