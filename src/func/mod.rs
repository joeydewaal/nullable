use sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, ObjectName,
};

use crate::{context::Context, expr::visit_expr};

pub fn visit_func(func: &Function, context: &mut Context) -> Option<bool> {
    let function_name = func_name(&func.name)?;
    let inferred_nullable = match function_name.to_lowercase().as_ref() {
        "count" | "current_user" | "now" | "random" | "version" => Some(false),
        "lower" | "upper" | "concat" | "length" | "abs" | "ceil" | "ceiling" | "floor"
        | "round" | "power" | "sum" | "avg" | "min" | "max" => {
            let nullables = args_nullables(&func.args, context);

            if nullables.len() > 0 && nullables.iter().all(|n| *n == Some(false)) {
                return Some(false);
            }
            None
        }
        "coalesce" => {
            let nullables = args_nullables(&func.args, context);

            if !nullables.is_empty() && nullables.iter().any(|n| *n == Some(false)) {
                return Some(false);
            }
            None
        }
        "array_agg" | "array_remove" => {
            let nullables = args_nullables(&func.args, context);

            if !nullables.is_empty() {
                return Some(false);
            } else {
                None
            }
        }
        _ => unimplemented!(),
    };

    inferred_nullable
}

fn args_nullables(args: &FunctionArguments, context: &mut Context) -> Vec<Option<bool>> {
    match args {
        FunctionArguments::List(list) => arg_list_nullable(&list, context),
        _ => unimplemented!(),
    }
}

fn arg_list_nullable(arg_list: &FunctionArgumentList, context: &mut Context) -> Vec<Option<bool>> {
    arg_list
        .args
        .iter()
        .map(|a| func_list_arg_nullable(a, context))
        .collect()
}

fn func_list_arg_nullable(arg: &FunctionArg, context: &mut Context) -> Option<bool> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => visit_expr(expr, None, context),
        _ => unimplemented!(),
    }
}

fn func_name(obj: &ObjectName) -> Option<String> {
    obj.0.first().map(|i| i.value.clone())
}
