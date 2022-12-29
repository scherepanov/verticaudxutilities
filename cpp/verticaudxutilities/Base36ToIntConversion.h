#pragma once
#include <Vertica.h>

namespace VerticaUDxUtilities {

class Base36ToInt : public Vertica::ScalarFunction {
  public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class Base36ToIntFactory : public Vertica::ScalarFunctionFactory {
  public:
    Base36ToIntFactory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class IntToBase36 : public Vertica::ScalarFunction {
  public:
    void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;
};

class IntToBase36Factory : public Vertica::ScalarFunctionFactory {
  public:
    IntToBase36Factory();

  public:
    void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
    void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
    Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
    void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

} // namespaces VerticaUDxUtilities
