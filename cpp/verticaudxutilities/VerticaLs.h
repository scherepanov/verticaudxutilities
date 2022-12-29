#pragma once
#include "Vertica.h"

namespace VerticaUDxUtilities {

  class VerticaLs : public Vertica::TransformFunction {
  public:
    void processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) override;
    void processFile(const std::string& dir, const char* file_name, Vertica::PartitionWriter &outputWriter);
  };

  class VerticaLsFactory : public Vertica::TransformFunctionFactory {
  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::TransformFunction *createTransformFunction(Vertica::ServerInterface &srvInterface) override;
  };

} // namespaces VerticaUDxUtilities
