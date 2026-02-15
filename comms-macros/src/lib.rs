use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, Meta, Lit, Expr};

#[proc_macro_attribute]
pub fn subscribe_rabbit(args: TokenStream, input: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(input as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_args = &input_fn.sig.inputs;
    
    let mut queue_name = String::new();
    
    let parser = syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated;
    let attr_args = parse_macro_input!(args with parser);
    
    for arg in attr_args {
        if let Meta::NameValue(nv) = arg {
            if nv.path.is_ident("queue") {
                if let Expr::Lit(syn::ExprLit { lit: Lit::Str(s), .. }) = nv.value {
                    queue_name = s.value();
                }
            }
        }
    }

    if queue_name.is_empty() {
        panic!("Missing 'queue' argument in #[subscribe_rabbit(queue = \"...\")]");
    }

    let first_arg = fn_args.first().expect("Function must have at least one argument");
    let req_type = match first_arg {
        syn::FnArg::Typed(pat_type) => &pat_type.ty,
        _ => panic!("Expected a typed argument"),
    };

    let expanded = quote! {
        #input_fn

        #[allow(non_camel_case_types)]
        pub struct #fn_name;

        #[async_trait::async_trait]
        impl comms::RabbitHandler for #fn_name {
            fn queue_name(&self) -> &str {
                #queue_name
            }

            async fn handle(&self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let request: #req_type = serde_json::from_slice(&data)?;
                #fn_name(request).await
            }
        }
    };

    TokenStream::from(expanded)
}
