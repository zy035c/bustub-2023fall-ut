#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"
#include "logic_expression.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) return optimized_plan;

  const auto &nlj_plan = dynamic_cast<NestedLoopJoinPlanNode &>(*optimized_plan);

  BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NestedLoopJoin must have two children");

  auto comparison_expression = dynamic_cast<ComparisonExpression *>(nlj_plan.Predicate().get());
  /**
   * Case 2: Compare Expression
   * <column expr> = <column expr>
  */
  if (comparison_expression != nullptr) {
    /**
     * the expression has shape of
     * compare, Equal
     * index 0: ColumnValueExpression
     * index 1: ColumnValueExpression
    */
    if (comparison_expression->comp_type_ != ComparisonType::Equal) return optimized_plan;

    auto column_value_expr_left = dynamic_cast<ColumnValueExpression *>(
      comparison_expression->GetChildAt(0).get()
    );
    if (column_value_expr_left == nullptr) return optimized_plan;

    auto column_value_expr_right = dynamic_cast<ColumnValueExpression *>(
      comparison_expression->GetChildAt(1).get()
    );
    if (column_value_expr_right == nullptr) return optimized_plan;

    /* Make new tuple vectors */
    auto left_expr_tuple_0 = std::make_shared<ColumnValueExpression>(
      0, 
      column_value_expr_left->GetColIdx(), 
      column_value_expr_left->GetReturnType()
    );
    auto right_expr_tuple_0 = std::make_shared<ColumnValueExpression>(
      0, 
      column_value_expr_right->GetColIdx(), 
      column_value_expr_right->GetReturnType()
    );
    std::vector<AbstractExpressionRef> left_expr_tuples{left_expr_tuple_0};
    std::vector<AbstractExpressionRef> right_expr_tuples{right_expr_tuple_0};
    /* end */

    /** Tuple index 0 = left side of join, tuple index 1 = right side of join */
    if (column_value_expr_left->GetTupleIdx() == 1 && column_value_expr_right->GetTupleIdx() == 0) {
      return std::make_shared<HashJoinPlanNode>(
        std::make_shared<const bustub::Schema>(nlj_plan.OutputSchema()),
        nlj_plan.GetLeftPlan(),
        nlj_plan.GetRightPlan(),
        right_expr_tuples, 
        left_expr_tuples,
        nlj_plan.GetJoinType()
      );
    } else if (column_value_expr_left->GetTupleIdx() == 0 && column_value_expr_right->GetTupleIdx() == 1) {
      return std::make_shared<HashJoinPlanNode>(
        std::make_shared<const bustub::Schema>(nlj_plan.OutputSchema()),
        nlj_plan.GetLeftPlan(),
        nlj_plan.GetRightPlan(),
        left_expr_tuples,
        right_expr_tuples,
        nlj_plan.GetJoinType()
      );
    }
  }

  /**
   * Case 2: Logic And Expression
   * <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  */
  auto logic_expression = dynamic_cast<LogicExpression *>(nlj_plan.Predicate().get());
  if (logic_expression != nullptr) {
    /**
     * the expression has shape of
     * and
     * index 0: ColumnValueExpression
     * index 1: ColumnValueExpression
    */
    if (logic_expression->logic_type_ != LogicType::And) {
      return optimized_plan;
    }

    /* Create two vectors and use lambda to recursively accumulate elements */
    std::vector<AbstractExpressionRef> left_expr_tuples;
    std::vector<AbstractExpressionRef> right_expr_tuples;

    std::function<void(AbstractExpression *)> process_compare;
    process_compare = [&](AbstractExpression *maybe_compare_expr) {

      if (maybe_compare_expr == nullptr) return;

      auto inner_recur_cmpr_expr = dynamic_cast<ComparisonExpression *>(maybe_compare_expr);
      if (inner_recur_cmpr_expr == nullptr) return;
      if (inner_recur_cmpr_expr->comp_type_ != ComparisonType::Equal) return;

      auto column_value_expr_left = dynamic_cast<ColumnValueExpression *>(
        inner_recur_cmpr_expr->GetChildAt(0).get()
      );
      if (column_value_expr_left == nullptr) return;

      auto column_value_expr_right = dynamic_cast<ColumnValueExpression *>(
        inner_recur_cmpr_expr->GetChildAt(1).get()
      );
      if (column_value_expr_right == nullptr) return;
      
      /* Append to tuple vectors */
      auto left_expr_tuple_0 = std::make_shared<ColumnValueExpression>(
        0, 
        column_value_expr_left->GetColIdx(),
        column_value_expr_left->GetReturnType()
      );
      auto right_expr_tuple_0 = std::make_shared<ColumnValueExpression>(
        0, 
        column_value_expr_right->GetColIdx(), 
        column_value_expr_right->GetReturnType()
      );

      if (column_value_expr_left->GetTupleIdx() == 0 && 
        column_value_expr_right->GetTupleIdx() == 1) {
          left_expr_tuples.push_back(left_expr_tuple_0);
          right_expr_tuples.push_back(right_expr_tuple_0);
      }

      if (column_value_expr_left->GetTupleIdx() == 1 && 
        column_value_expr_right->GetTupleIdx() == 0) {
          left_expr_tuples.push_back(right_expr_tuple_0);
          right_expr_tuples.push_back(left_expr_tuple_0);
      }
      /* end */
    };

    std::function<void(LogicExpression *)> recur; /* Define this recur lambda function */  
    recur = [&](LogicExpression *logic_expr) {

      auto left_expr = logic_expr->GetChildAt(0);

      // recursive case 1: if left expr is a logic expr
      auto left_expr_logic = dynamic_cast<LogicExpression *>(left_expr.get());
      if (left_expr_logic != nullptr) {
        recur(left_expr_logic);
      }

      // recursive case 2: if left expr is a compare expr
      process_compare(left_expr.get());

      auto right_expr = logic_expr->GetChildAt(1);

      // recursive case 1: if right expr is a logic expr
      auto right_expr_logic = dynamic_cast<LogicExpression *>(right_expr.get());
      if (right_expr_logic != nullptr) {
        recur(right_expr_logic);
      }

      // recursive case 2: if right expr is a compare expr
      process_compare(right_expr.get());
    };

    // call recursive func
    recur(logic_expression);

    /* gather from 2 vectors */
    return std::make_shared<HashJoinPlanNode>(
      std::make_shared<const bustub::Schema>(nlj_plan.OutputSchema()), 
      nlj_plan.GetLeftPlan(), 
      nlj_plan.GetRightPlan(),
      left_expr_tuples, 
      right_expr_tuples, 
      nlj_plan.GetJoinType()
    );
  }

  return optimized_plan;
}

}  // namespace bustub
