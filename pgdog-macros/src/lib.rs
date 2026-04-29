//! Macros used by PgDog plugins.
//!
//! Required and exported by the `pgdog-plugin` crate. You don't have to add this crate separately.
//!
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
        pub unsafe extern "C" fn pgdog_rustc_version(output: *mut pgdog_plugin::PdStr) {
            let version = pgdog_plugin::comp::rustc_version();
            unsafe {
                *output = version;
            }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn pgdog_plugin_version(output: *mut pgdog_plugin::PdStr) {
            let version: pgdog_plugin::PdStr = env!("CARGO_PKG_VERSION").into();
            unsafe {
                *output = version;
            }
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn pgdog_plugin_api_version(output: *mut pgdog_plugin::PdStr) {
            let version = pgdog_plugin::comp::pgdog_plugin_api_version();
            unsafe {
                *output = version;
            }
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn pgdog_logging_init(config: pgdog_plugin::PdConfig) {
            pgdog_plugin::logging::init(&config);
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

/// Generate the `pgdog_config` method that's executed at plugin load time.
#[proc_macro_attribute]
pub fn config(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;

    let expanded = quote! {

        #[unsafe(no_mangle)]
        pub extern "C" fn pgdog_config(
            pd_config: pgdog_plugin::PdConfig,
            result: *mut u8)
        {
            #input_fn

            #fn_name(pd_config, result);
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

/// Generates the `pgdog_route_copy_row` method for routing COPY rows.
///
/// The decorated function receives a [`PdCopyRow`] and returns a [`Route`].
///
/// ### Example
///
/// ```ignore
/// use pgdog_plugin::prelude::*;
///
/// #[route_copy_row]
/// fn route_copy_row(row: PdCopyRow) -> Route {
///     Route::unknown()
/// }
/// ```
#[proc_macro_attribute]
pub fn route_copy_row(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_inputs = &input_fn.sig.inputs;

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
        .expect("route_copy_row function must have at least one named parameter");

    let expanded = quote! {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn pgdog_route_copy_row(#first_param_name: pgdog_plugin::PdCopyRow, output: *mut pgdog_plugin::PdRoute) {
            #input_fn

            let route: pgdog_plugin::PdRoute = #fn_name(#first_param_name).into();
            unsafe {
                *output = route;
            }
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
        pub unsafe extern "C" fn pgdog_route(#first_param_name: pgdog_plugin::PdRouterContext, output: *mut pgdog_plugin::PdRoute) {
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
