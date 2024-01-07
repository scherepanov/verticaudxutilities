#include <atomic>
#include <unistd.h>
#include "Args.h"

namespace VerticaUDxUtilities {

static std::atomic_int instance_counter(0);
static std::atomic_int active_instance_count(0);

Args::Args(): debug(false), info(false), instance_no(0) {
  instance_no = instance_counter.fetch_add(1);
  active_instance_count++;
}

Args::~Args() {
  active_instance_count--;
}

void ArgsFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addAny();
  returnType.addAny();
}

void ArgsFactory::getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) {
  parameterTypes.addBool("info");
  parameterTypes.addBool("debug");
  parameterTypes.addInt("order_by_column_count");
}

void ArgsFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  auto params = srvInterface.getParamReader();
  size_t order_by_column_count = (params.containsParameter("order_by_column_count") ? params.getIntRef( "order_by_column_count" ) : 0);
  bool debug = params.containsParameter("debug") && params.getBoolRef("debug");
  std::vector<size_t> arg_cols;
  inputTypes.getArgumentColumns(arg_cols);
  if(order_by_column_count > arg_cols.size()) {
    vt_report_error(101, "Parameter order_by_column_count %ld exceed number of columns %ld", order_by_column_count, arg_cols.size());
  }
  size_t ind = 0;
  for (const auto & col : arg_cols) {
    Vertica::SizedColumnTypes::Properties props;
    if (ind < order_by_column_count) {
        if (debug) {
            srvInterface.log("Adding order by column %s ind %lu", inputTypes.getColumnName(col).c_str(), col);
        }
        props.isSortedBy = true;
    }
    outputTypes.addArg(inputTypes.getColumnType(col), inputTypes.getColumnName(col), props);
    ind++;
  }
  if(srvInterface.getParamReader().containsParameter("info")) {
    outputTypes.addVarchar(32, "node_name");
    outputTypes.addInt("instance_no");
    outputTypes.addInt("active_instance_count");
    outputTypes.addInt("Instances_executing");
  }
}

Vertica::TransformFunction * ArgsFactory::createTransformFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, Args);
}

void Args::setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes){
  auto params = srvInterface.getParamReader();
  info = params.containsParameter("info") && params.getBoolRef("info");
  debug = params.containsParameter("debug") && params.getBoolRef("debug");
}

void Args::processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) {
  static std::atomic_int instances_running(0);
  int inr = 0;
  if (info) {
    instances_running++;
    inr = instances_running;
    if (debug) {
      srvInterface.log("process partition start, instances_running %d", inr);
    }
  }
  std::vector<size_t> arg_cols;
  Vertica::SizedColumnTypes meta = inputReader.getTypeMetaData();
  meta.getArgumentColumns(arg_cols);
  size_t part_row_cnt = 0;
  do {
    size_t col_idx = 0;
    for( const auto& col : arg_cols ) {
      outputWriter.copyFromInput( col_idx, inputReader, col );
      col_idx++;
    }
    if( info ) {
      outputWriter.getStringRef( col_idx++ ).copy( srvInterface.getCurrentNodeName().c_str() );
      outputWriter.setInt( col_idx++, instance_no );
      outputWriter.setInt( col_idx++, active_instance_count );
      outputWriter.setInt( col_idx, inr );
    }
    outputWriter.next();
    part_row_cnt++;
  } while ( inputReader.next() );
  if(debug) {
    srvInterface.log("process partition   end, instances_running %d, rows processed %ld", inr, part_row_cnt);
  }
  if (info) {
    instances_running--;
  }
}

RegisterFactory(ArgsFactory);

} // namespaces VerticaUDxUtilities
