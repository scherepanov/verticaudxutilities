#pragma once
#include <Vertica.h>

namespace VerticaUDxUtilities {

class SQLDigestFactory : public Vertica::ScalarFunctionFactory {
public:
  SQLDigestFactory();

public:
  void getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType) override;
  void getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes) override;
  void getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) override;
  Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &srvInterface) override;
  void getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) override;
};

class SQLDigest : public Vertica::ScalarFunction {

public:
  SQLDigest();
public:
  void setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes & 	argTypes) override;
  void processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) override;

public:
  [[nodiscard]] static inline bool isWhitespace(const char c) {
    return c == ' ' || c == '\t' || c == '\n';
  }

  [[nodiscard]] static inline bool isDigit(const char c) {
    return c >= '0' && c <= '9';
  }

  [[nodiscard]] static inline bool isDelimiter(const char c) {
    //"\n,.$_()><=!|+-*/%"
    return c == '\n'
      || c == ','
      || c == '.'
      || c == '$'
      || c == '_'
      || c == '('
      || c == ')'
      || c == '>'
      || c == '<'
      || c == '='
      || c == '!'
      || c == '|'
      || c == '+'
      || c == '-'
      || c == '*'
      || c == '/'
      || c == '%';
  }

  [[nodiscard]] static inline bool isDot(const char c) {
    return c == '.';
  }

  void appendNum(char* data, size_t& ptr, size_t& ind, char prefix);

public:
  void staticStr(const Vertica::VString& source_str);
  void staticInt();
  uint64_t staticHash();

private:
  std::vector<std::string> numbers;
  std::vector<char> buffer_int;
  std::vector<char> buffer_str;
  size_t buffer_int_len;
  size_t buffer_str_len;
  bool only_static_str;
  bool only_static_int;
  bool debug;
};

}