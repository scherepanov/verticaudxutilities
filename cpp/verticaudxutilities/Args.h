#pragma once
#include "Vertica.h"

namespace VerticaUDxUtilities {

class Args : public Vertica::TransformFunction {

  public:
    Args();
    ~Args() override;

  public:
    void setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes) override;
    void processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) override;

  private:
    bool debug;
    bool info;
    long instance_no;
};

class ArgsFactory : public Vertica::TransformFunctionFactory {
  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    void getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) override;
    Vertica::TransformFunction *createTransformFunction(Vertica::ServerInterface &srvInterface) override;
};

} // namespace VerticaUDxUtilities

