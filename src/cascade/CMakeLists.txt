add_library_unity(duckdb_cascade OBJECT aggregate_pullup.cpp aggregate_pushdown.cpp apply_decorrelation.cpp
                  cascade_bindings.cpp cascade_correlation.cpp
                  cascades/memo.cpp cascades/task.cpp cascades/cost.cpp cascades/search.cpp
                  cascades/rules/push_filter_below_groupby.cpp
	cascades/rules/left_outer_apply_to_join.cpp
	cascades/rules/binder_side_invariants.cpp
	cascades/rules/select_2_filter.cpp
	cascades/rules/expand_nary_join.cpp cascades/rules/apply_to_join.cpp cascades/rules/lift_local_predicate.cpp cascades/rules/semi_apply_to_join.cpp cascades/rules/correlated_apply_to_join.cpp cascades/rules/group_apply_by_outer_columns.cpp cascades/rules/collapse_project.cpp
                  apply_decorrelation_setop.cpp apply_decorrelation_crossproduct.cpp apply_decorrelation_scalar.cpp
                  cascade_config.cpp cascade_keys.cpp cascade_optimizer.cpp
                  groupby_reorder.cpp local_aggregate.cpp simplify_markers.cpp
                  segment_apply.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_cascade>
    PARENT_SCOPE)
