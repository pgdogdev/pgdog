//! Macros used by PgDog plugins.
//!
//! Required and exported by the `pgdog-plugin` crate. You don't have to add this crate separately.
//!
use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, LitInt, parse_macro_input};

/// Default number of attempts when `#[flaky]` is used without an explicit count.
const DEFAULT_FLAKY_ATTEMPTS: usize = 3;

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

/// Retry a flaky test until it passes or the attempt budget is exhausted.
///
/// Place this **above** the test attribute. It works for both synchronous
/// (`#[test]`) and asynchronous (`#[tokio::test]`) tests:
///
/// ```ignore
/// #[flaky]            // retries up to 3 times (the default)
/// #[tokio::test]
/// async fn sometimes_flaky() {
///     assert!(roll_the_dice());
/// }
///
/// #[flaky(5)]         // retries up to 5 times
/// #[test]
/// fn also_flaky() {
///     assert!(roll_the_dice());
/// }
/// ```
///
/// Each attempt that panics is caught and logged to stderr, then the test is
/// retried. The final attempt is run uncaught so that a genuine failure
/// propagates with its original panic message and backtrace.
///
/// The async variant relies on `futures` and `std` being available in the
/// consuming crate (both are present in PgDog).
#[proc_macro_attribute]
pub fn flaky(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attempts: usize = if attr.is_empty() {
        DEFAULT_FLAKY_ATTEMPTS
    } else {
        let lit = parse_macro_input!(attr as LitInt);
        match lit.base10_parse() {
            Ok(0) | Ok(1) => 1,
            Ok(n) => n,
            Err(err) => return err.to_compile_error().into(),
        }
    };

    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = parse_macro_input!(item as ItemFn);

    let test_name = sig.ident.to_string();

    // The last attempt is run uncaught so a real failure keeps its original
    // panic message and backtrace. Earlier attempts are caught and retried.
    let retried = attempts.saturating_sub(1);

    let body = if sig.asyncness.is_some() {
        quote! {
            use ::futures::future::FutureExt;
            for __flaky_attempt in 1..=#retried {
                match ::std::panic::AssertUnwindSafe(async #block).catch_unwind().await {
                    ::std::result::Result::Ok(__flaky_value) => return __flaky_value,
                    ::std::result::Result::Err(_) => {
                        eprintln!(
                            "[flaky] test `{}` failed on attempt {}/{}, retrying...",
                            #test_name, __flaky_attempt, #attempts,
                        );
                    }
                }
            }
            (async #block).await
        }
    } else {
        quote! {
            for __flaky_attempt in 1..=#retried {
                match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(|| #block)) {
                    ::std::result::Result::Ok(__flaky_value) => return __flaky_value,
                    ::std::result::Result::Err(_) => {
                        eprintln!(
                            "[flaky] test `{}` failed on attempt {}/{}, retrying...",
                            #test_name, __flaky_attempt, #attempts,
                        );
                    }
                }
            }
            #block
        }
    };

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            #body
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
