use sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, ObjectName,
};

use crate::{expr::visit_expr, Tables};

pub fn visit_func(func: &Function, tables: &Tables) -> Option<bool> {
    let function_name = func_name(&func.name)?;
    let inferred_nullable = match function_name.to_lowercase().as_ref() {
        "count" | "current_user" | "now" | "random" | "version" => Some(false),
        "lower" | "upper" | "concat" | "length" | "abs" | "ceil" | "ceiling" | "floor"
        | "round" | "power" | "sum" | "avg" | "min" | "max" => {
            let nullables = args_nullables(&func.args, tables);

            if nullables.len() > 0 && nullables.iter().all(|n| *n == Some(false)) {
                return Some(false);
            }
            None
        }
        "coalesce" => {
            let nullables = args_nullables(&func.args, tables);

            if nullables.len() > 0 && nullables.iter().any(|n| *n == Some(false)) {
                return Some(false);
            }
            None
        }
        _ => None,
    };

    inferred_nullable
}

fn args_nullables(args: &FunctionArguments, tables: &Tables) -> Vec<Option<bool>> {
    match args {
        FunctionArguments::List(list) => arg_list_nullable(&list, tables),
        _ => Vec::default(),
    }
}

fn arg_list_nullable(arg_list: &FunctionArgumentList, tables: &Tables) -> Vec<Option<bool>> {
    arg_list
        .args
        .iter()
        .map(|a| func_list_arg_nullable(a, tables))
        .collect()
}

fn func_list_arg_nullable(arg: &FunctionArg, tables: &Tables) -> Option<bool> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => visit_expr(expr, None, tables),
        _ => None,
    }
}

fn func_name(obj: &ObjectName) -> Option<String> {
    obj.0.first().map(|i| i.value.clone())
}
