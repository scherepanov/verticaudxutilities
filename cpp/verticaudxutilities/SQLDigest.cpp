#include "SQLDigest.h"

namespace VerticaUDxUtilities {

constexpr size_t BUFFER_MAX_SIZE = 4000 + 12000;
constexpr size_t MAX_NUMBERS = 1001;

SQLDigestFactory::SQLDigestFactory(){
  strict = Vertica::RETURN_NULL_ON_NULL_INPUT;
  vol = Vertica::IMMUTABLE;
}

void SQLDigestFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addVarchar();
  returnType.addAny();
}

void SQLDigestFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  bool only_static_str = srvInterface.getParamReader().containsParameter("only_static_str");
  bool only_static_int = srvInterface.getParamReader().containsParameter("only_static_int");
  bool debug = srvInterface.getParamReader().containsParameter("debug");

  if(only_static_str) {
    outputTypes.addVarchar(BUFFER_MAX_SIZE, "static_str");
    if(debug) {
      srvInterface.log("Output type static_str");
    }
  } else if(only_static_int) {
    outputTypes.addVarchar(BUFFER_MAX_SIZE, "static_int");
    if(debug) {
      srvInterface.log("Output type static_str");
    }
  } else {
    outputTypes.addInt("digest");
    if(debug) {
      srvInterface.log("Output type int digest");
    }
  }
}

void SQLDigestFactory::getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) {
  parameterTypes.addVarchar(32, "debug");
  parameterTypes.addVarchar(32, "only_static_str");
  parameterTypes.addVarchar(32, "only_static_int");
}

Vertica::ScalarFunction * SQLDigestFactory::createScalarFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, SQLDigest);
}

void SQLDigestFactory::getPerInstanceResources(Vertica::ServerInterface& srvInterface, Vertica::VResources& res) {
  res.nFileHandles = 0;
  res.scratchMemory = 40*1024;
}

SQLDigest::SQLDigest () :
  buffer_int_len(),
  buffer_str_len(),
  only_static_str(),
  only_static_int(),
  debug()
{
  buffer_str.reserve(BUFFER_MAX_SIZE);
  buffer_int.reserve(BUFFER_MAX_SIZE);
  numbers.clear();
  numbers.reserve(1001);
  for (size_t i = 0; i < MAX_NUMBERS; i++) {
    numbers.emplace_back(std::to_string(i));
  }
}

void SQLDigest::appendNum(char* data, size_t& ptr, size_t& ind, char prefix) {
  data[ptr++] = ':';
  data[ptr++] = prefix;
  if(ind < MAX_NUMBERS) {
    std::memcpy(data + ptr, numbers[ind].data(), numbers[ind].length());
    ptr += numbers[ind].length();
  } else {
    std::memcpy(data + ptr, numbers[0].data(), numbers[0].length());
    ptr += numbers[0].length();
  }
  ind++;
  data[ptr++] = ' ';
}

void SQLDigest::staticStr(const Vertica::VString& source_str) {
  bool str_literal = false;
  bool str_literal_end = false;
  size_t literal_ind = 0;
  size_t source_len = source_str.length();
  if(source_len > 4000) {
    source_len = 4000;
  }
  const char* source_data = source_str.data();
  char* dest_data = buffer_str.data();
  buffer_str_len = 0;
  for (size_t ind = 0; ind < source_len; ind++) {
    char c = source_data[ind];
    if(c >= 'A' && c <= 'Z') {
      c += 32;
    }
    if(str_literal) {
      if (c == (char)39) {
        str_literal = false;
        str_literal_end = true;
      }
    } else {
      if (c == (char)39) {
        str_literal = true;
        if(!str_literal_end) {
          appendNum(dest_data, buffer_str_len, literal_ind, 's');
        }
      } else {
        dest_data[buffer_str_len++] = c;
      }
      str_literal_end = false;
    }
  }
  dest_data[buffer_str_len] = 0;
}

void SQLDigest::staticInt() {
  bool str_int = false;
  bool prev_delimiter = true;
  bool str_whitespace = true;
  size_t literal_ind = 0;
  buffer_int_len = 0;
  const char* src_data = buffer_str.data();
  char* dest_data = buffer_int.data();
  for (size_t ind = 0; ind < buffer_str_len; ind++) {
    char c = *(src_data + ind);
    bool is_whitespace = isWhitespace(c);
    bool is_digit = isDigit(c);
    if (str_int && (is_digit || isDot(c))) {
      continue;
    }
    str_int = false;
    if(str_whitespace && is_whitespace) {
      continue;
    }
    str_whitespace = false;
    if(is_whitespace) {
      str_whitespace = true;
      prev_delimiter = true;
      dest_data[buffer_int_len++] = ' ';
      continue;
    }
    if(isDelimiter(c)) {
      prev_delimiter = true;
      dest_data[buffer_int_len++] = c;
      continue;
    }
    if (is_digit && prev_delimiter) {
      str_int = true;
      appendNum(dest_data, buffer_int_len, literal_ind, 'i');
    } else {
      dest_data[buffer_int_len++] = c;
    }
    prev_delimiter = false;
  }
  if(!str_whitespace){
    dest_data[buffer_int_len++] = ' ';
  }
  dest_data[buffer_int_len] = 0;
}

uint64_t SQLDigest::staticHash() {
  uint64_t hash = 0;
  size_t off = 0;
  char* src_data = buffer_int.data();
  while (buffer_int_len >= off + 8) {
    hash ^= *reinterpret_cast<uint64_t*>(src_data + off);
    off += 8;
  }
  size_t left = buffer_int_len - off;
  if(left > 0) {
    uint64_t t = 0;
    std::memcpy(&t, src_data + off, left);
    hash ^= t;
  }
  return hash;
}

void SQLDigest::setup(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes& argTypes) {
  only_static_str = srvInterface.getParamReader().containsParameter("only_static_str");
  only_static_int = srvInterface.getParamReader().containsParameter("only_static_int");
  debug = srvInterface.getParamReader().containsParameter("debug");
}

void SQLDigest::processBlock(Vertica::ServerInterface &srvInterface, Vertica::BlockReader &arg_reader, Vertica::BlockWriter &res_writer) {
  try {
    do {
      if(arg_reader.isNull(0)) {
        res_writer.setNull();
      } else {
        const Vertica::VString& source_str = arg_reader.getStringRef(0);
        if(debug) {
          srvInterface.log("Source str %s", source_str.data());
        }
        staticStr(source_str);
        if(debug) {
          srvInterface.log("Static str %s len %ld", buffer_str.data(), buffer_str_len);
        }
        if(only_static_str) {
          res_writer.getStringRef(0).copy(buffer_str.data(), buffer_str_len);
          res_writer.next();
          continue;
        }
        staticInt();
        if(debug) {
          srvInterface.log("Static int %s len %ld", buffer_int.data(), buffer_int_len);
        }
        if(only_static_int) {
          res_writer.getStringRef(0).copy(buffer_int.data(), buffer_int_len);
          res_writer.next();
          continue;
        }
        uint64_t hash = staticHash();
        if(debug) {
          srvInterface.log("Hash %ld", hash);
        }
        res_writer.setInt(static_cast<long>(hash));
      }
      res_writer.next();
    } while ( arg_reader.next() );
  } catch(const std::exception& e) {
    vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(SQLDigestFactory);

}