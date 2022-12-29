#pragma once
#include <Vertica.h>

namespace VerticaUDxUtilities {

class BinaryToChar : public Vertica::ScalarFunction {
  public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class BinaryToCharFactory : public Vertica::ScalarFunctionFactory {
  public:
    BinaryToCharFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class CharToBinary : public Vertica::ScalarFunction {
  public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class CharToBinaryFactory : public Vertica::ScalarFunctionFactory {
  public:
    CharToBinaryFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

} // namespaces VerticaUDxUtilities
