#include <cudf/column/column.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/types.hpp>
#include <rmm/device_buffer.hpp>

int main() {
  auto col = cudf::make_numeric_column(
      cudf::data_type{cudf::type_id::INT32}, 10);
  return 0;
}