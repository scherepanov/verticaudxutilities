#pragma once
#include "Vertica.h"

namespace VerticaUDxUtilities {

  class MPArgsPhase : public Vertica::TransformFunctionPhase {
  public:
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  };

  class MPArgsPhase1 : public MPArgsPhase {
  public:
    Vertica::TransformFunction *createTransformFunction(Vertica::ServerInterface &srvInterface) override;
  };

  class MPArgsPhase2 : public MPArgsPhase {
  public:
    Vertica::TransformFunction *createTransformFunction(Vertica::ServerInterface &srvInterface) override;
  };

  class MPArgsPhase3 : public MPArgsPhase {
  public:
    Vertica::TransformFunction *createTransformFunction(Vertica::ServerInterface &srvInterface) override;
  };

  class MPArgsFactory : public Vertica::MultiPhaseTransformFunctionFactory {
  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) override;
    void getPhases(Vertica::ServerInterface &srvInterface, std::vector<Vertica::TransformFunctionPhase *> &phases) override;

  private:
    MPArgsPhase1 phase1;
    MPArgsPhase2 phase2;
    MPArgsPhase3 phase3;
  };

  class MPArgs : public Vertica::TransformFunction {

  public:
    explicit MPArgs(std::string phase_name);
    ~MPArgs() override;

  public:
    void setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes) override;
    void processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) override;
    void destroy(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes) override;

  private:
    std::string phase_name;
    bool debug;
    bool info;
    long delay_ms;
    long instance_no;
  };

  class MPArgs1 : public MPArgs {
  public:
    MPArgs1();
  };

  class MPArgs2 : public MPArgs {
  public:
    MPArgs2();
  };

  class MPArgs3 : public MPArgs {
  public:
    MPArgs3();
  };

}