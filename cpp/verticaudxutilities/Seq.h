#pragma once
#include "Vertica.h"

namespace VerticaUDxUtilities {

class Seq : public Vertica::TransformFunction {

  public:
    void processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) override;

};

class SeqFactory : public Vertica::TransformFunctionFactory {
  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    void getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) override;
    Vertica::TransformFunction *createTransformFunction(Vertica::ServerInterface &srvInterface) override;
};

} // namespaces VerticaUDxUtilities
