#include <atomic>
#include <unistd.h>
#include "Args.h"

namespace VerticaUDxUtilities {

static std::atomic_int instance_counter(0);
static std::atomic_int active_instance_count(0);

Args::Args(): debug(false), info(false), delay_ms(0), repeat(1) {
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
  parameterTypes.addVarchar(32, "info");
  parameterTypes.addInt("delay_ms");
  parameterTypes.addInt("repeat");
}

void ArgsFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  std::vector<size_t> arg_cols;
  inputTypes.getArgumentColumns(arg_cols);
  for (const auto & col : arg_cols) {
    outputTypes.addArg(inputTypes.getColumnType(col), inputTypes.getColumnName(col));
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
  info = srvInterface.getParamReader().containsParameter("info");
  if(srvInterface.getParamReader().containsParameter("delay_ms")) {
    delay_ms = srvInterface.getParamReader().getIntRef("delay_ms");
    if(delay_ms > 10000 || delay_ms < 0)
      vt_report_error(1, "Delay too long - why you need delay more than 10 sec?");
  }
  if(srvInterface.getParamReader().containsParameter("repeat")) {
    repeat = srvInterface.getParamReader().getIntRef( "repeat" );
  }
}

void Args::processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) {
  static std::atomic_int instances_running(0);
  instances_running++;
  int inr = instances_running;
  if(debug) srvInterface.log("process partition, instances_running %d", inr);
  if(delay_ms != 0) {
    usleep(delay_ms * 1000);
  }
  try {
    std::vector<size_t> arg_cols;
    Vertica::SizedColumnTypes meta = inputReader.getTypeMetaData();
    meta.getArgumentColumns(arg_cols);
    do {
      for (int repeat_ind = 0; repeat_ind < repeat; repeat_ind++) {
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
      }
    } while ( inputReader.next() );
  } catch(const std::exception& e) {
    instances_running--;
    vt_report_error(0, "Exception: [%s]", e.what());
  }
  instances_running--;
}

RegisterFactory(ArgsFactory);

} // namespaces VerticaUDxUtilities
