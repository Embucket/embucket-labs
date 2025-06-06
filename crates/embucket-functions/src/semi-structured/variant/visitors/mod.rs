pub mod type_rewrite;
pub mod variant_element;

use datafusion_expr::sqlparser::ast::Statement;
pub mod array_agg;
pub mod array_construct;
pub mod array_construct_compact;

pub fn visit_all(stmt: &mut Statement) {
    array_agg::visit(stmt);
    array_construct_compact::visit(stmt);
    array_construct::visit(stmt);
    type_rewrite::visit(stmt);
    variant_element::visit(stmt);
}
