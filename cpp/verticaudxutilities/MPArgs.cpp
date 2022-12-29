#include <atomic>
#include <unistd.h>
#include "MPArgs.h"

namespace VerticaUDxUtilities {

  void MPArgsFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
    argTypes.addAny();
    returnType.addAny();
  }

  void MPArgsFactory::getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) {
    parameterTypes.addVarchar(32, "info");
    parameterTypes.addVarchar(32, "debug");
  }

  void MPArgsFactory::getPhases(Vertica::ServerInterface &srvInterface, std::vector<Vertica::TransformFunctionPhase *> &phases)
  {
    phase1.setPrepass();
    phases.push_back(&phase1);
    phases.push_back(&phase2);
    phases.push_back(&phase3);
  }

  RegisterFactory(MPArgsFactory);

  void MPArgsPhase::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) {
    std::vector<size_t> arg_cols;
    inputTypes.getArgumentColumns(arg_cols);
    for (const auto & col : arg_cols) {
      outputTypes.addArg(inputTypes.getColumnType(col), inputTypes.getColumnName(col));
    }
    if(srvInterface.getParamReader().containsParameter("info")) {
      outputTypes.addVarchar(32, "phase_name");
      outputTypes.addVarchar(32, "node_name");
      outputTypes.addInt("instance_no");
      outputTypes.addInt("active_instance_count");
      outputTypes.addInt("Instances_executing");
    }
  }

  Vertica::TransformFunction* MPArgsPhase1::createTransformFunction(Vertica::ServerInterface &srvInterface) {
    return vt_createFuncObj(srvInterface.allocator, MPArgs1);
  }

  Vertica::TransformFunction* MPArgsPhase2::createTransformFunction(Vertica::ServerInterface &srvInterface) {
    return vt_createFuncObj(srvInterface.allocator, MPArgs2);
  }

  Vertica::TransformFunction* MPArgsPhase3::createTransformFunction(Vertica::ServerInterface &srvInterface) {
    return vt_createFuncObj(srvInterface.allocator, MPArgs3);
  }

  static std::atomic_int mpargs_instance_counter(0);
  static std::atomic_int mpargs_active_instance_count(0);

  MPArgs::MPArgs(std::string phase_name):
      phase_name{std::move(phase_name)},
      debug(),
      info(),
      delay_ms() {
    instance_no = mpargs_instance_counter.fetch_add(1);
    mpargs_active_instance_count++;
  }

  MPArgs::~MPArgs() {
    mpargs_active_instance_count--;
  }


  void MPArgs::setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes){
    info = srvInterface.getParamReader().containsParameter("info");
    debug = srvInterface.getParamReader().containsParameter("debug");
    if(debug) {
      srvInterface.log("Setup %s", phase_name.c_str());
    }
    if(srvInterface.getParamReader().containsParameter("delay_ms")) {
      delay_ms = srvInterface.getParamReader().getIntRef("delay_ms");
      if(delay_ms > 10000 || delay_ms < 0)
        vt_report_error(1, "Delay too long - why you need delay more than 10 sec?");
    }
  }

  void MPArgs::destroy(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes) {
    if(debug) {
      srvInterface.log("Destroy %s", phase_name.c_str());
    }
  }

  void MPArgs::processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) {
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
        size_t col_idx = 0;
        for(const auto& col : arg_cols) {
          outputWriter.copyFromInput(col_idx, inputReader, col);
          col_idx++;
        }
        if(info) {
          outputWriter.getStringRef(col_idx++).copy(phase_name.c_str());
          outputWriter.getStringRef(col_idx++).copy(srvInterface.getCurrentNodeName().c_str());
          outputWriter.setInt(col_idx++, instance_no);
          outputWriter.setInt(col_idx++, mpargs_active_instance_count);
          outputWriter.setInt(col_idx, inr);
        }
        outputWriter.next();
      } while ( inputReader.next() );
    } catch(const std::exception& e) {
      instances_running--;
      vt_report_error(0, "Exception: [%s]", e.what());
    }
    instances_running--;
  }

  MPArgs1::MPArgs1():
    MPArgs("phase 1")
  {}

  MPArgs2::MPArgs2():
    MPArgs("phase 2")
  {}

  MPArgs3::MPArgs3():
    MPArgs("phase 3")
  {}

}
