use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

/// Generates required methods for PgDog to run at plugin load time.
///
/// ### Methods
///
/// * `pgdog_rustc_version`: Returns the version of the Rust compiler used to build the plugin.
/// * `pgdog_pg_query_version`: Returns the version of the pg_query library used by the plugin.
/// * `pgdog_plugin_version`: Returns the version of the plugin itself, taken from Cargo.toml.
///
#[proc_macro]
pub fn plugin(_input: TokenStream) -> TokenStream {
    let expanded = quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn pgdog_rustc_version(output: *mut pgdog_plugin::PdStr) {
            let version = pgdog_plugin::comp::rustc_version();
            unsafe {
                *output = version;
            }
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn pgdog_pg_query_version(output: *mut pgdog_plugin::PdStr) {
            let version: pgdog_plugin::PdStr = option_env!("PGDOG_PGQUERY_VERSION")
                .unwrap_or_default()
                .into();
            unsafe {
                *output = version;
            }
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn pgdog_plugin_version(output: *mut pgdog_plugin::PdStr) {
            let version: pgdog_plugin::PdStr = env!("CARGO_PKG_VERSION").into();
            unsafe {
                *output = version;
            }
        }
    };
    TokenStream::from(expanded)
}

/// Generate the `pgdog_init` method that's executed at plugin load time.
#[proc_macro_attribute]
pub fn init(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;

    let expanded = quote! {

        #[unsafe(no_mangle)]
        pub extern "C" fn pgdog_init() {
            #input_fn

            #fn_name();
        }
    };

    TokenStream::from(expanded)
}

/// Generate the `pgdog_fini` method that runs at PgDog shutdown.
#[proc_macro_attribute]
pub fn fini(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;

    let expanded = quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn pgdog_fini() {
            #input_fn

            #fn_name();
        }
    };

    TokenStream::from(expanded)
}

/// Generates the `pgdog_route` method for routing queries.
#[proc_macro_attribute]
pub fn route(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_inputs = &input_fn.sig.inputs;

    // Extract the first parameter name and type for the pgdog_route function signature
    let (first_param_name, _) = fn_inputs
        .iter()
        .filter_map(|input| {
            if let syn::FnArg::Typed(pat_type) = input {
                if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                    Some((pat_ident.ident.clone(), pat_type.ty.clone()))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .next()
        .expect("Route function must have at least one named parameter");

    let expanded = quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn pgdog_route(#first_param_name: pgdog_plugin::PdRouterContext, output: *mut pgdog_plugin::PdRoute) {
            #input_fn

            let pgdog_context: pgdog_plugin::Context = #first_param_name.into();
            let route: pgdog_plugin::PdRoute = #fn_name(pgdog_context).into();
            unsafe {
                *output = route;
            }
        }
    };

    TokenStream::from(expanded)
}
