add_library_unity(duckdb_func_pragma OBJECT pragma_functions.cpp git4data_pragmas.cpp git4data_pragma_lines.cpp git4data_pragma_pick.cpp git4data_pragma_apply.cpp git4data_pragma_mappings.cpp git4data_pragma_references.cpp
                  pragma_queries.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_pragma>
    PARENT_SCOPE)
