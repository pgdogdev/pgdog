use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

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

#[proc_macro_attribute]
pub fn init(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;

    let expanded = quote! {
        #input_fn

        #[unsafe(no_mangle)]
        pub extern "C" fn pgdog_init() {
            #fn_name();
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn fini(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;

    let expanded = quote! {
        #input_fn

        #[unsafe(no_mangle)]
        pub extern "C" fn pgdog_fini() {
            #fn_name();
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn route(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_inputs = &input_fn.sig.inputs;

    // Extract parameter names for the function call
    let param_names: Vec<_> = fn_inputs
        .iter()
        .filter_map(|input| {
            if let syn::FnArg::Typed(pat_type) = input {
                if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                    Some(&pat_ident.ident)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let expanded = quote! {
        #input_fn

        #[unsafe(no_mangle)]
        pub extern "C" fn pgdog_route(context: pgdog_plugin::PdRouterContext, output: *mut PdRoute) {
            let route = #fn_name(#(#param_names),*);
            unsafe {
                *output = route;
            }
        }
    };

    TokenStream::from(expanded)
}
