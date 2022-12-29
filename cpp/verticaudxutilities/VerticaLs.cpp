#include "VerticaLs.h"
#include "UDxUtilities.h"
#include <dirent.h>

namespace VerticaUDxUtilities {

  void VerticaLsFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
    argTypes.addVarchar();
    returnType.addVarchar();      // 0 - filename
    returnType.addChar();         // 1 - filetype
    returnType.addTimestampTz();  // 2 - timestampTz
    returnType.addInt();          // 3 - size
  }

  void VerticaLsFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
    UDxUtilities::check_argument_count(inputTypes, 1);
    outputTypes.addVarchar(256, "filename");
    outputTypes.addChar(1, "filetype");
    outputTypes.addTimestampTz(0, "timestamp");
    outputTypes.addInt("size");
  }

  Vertica::TransformFunction * VerticaLsFactory::createTransformFunction(Vertica::ServerInterface &srvInterface){
    return vt_createFuncObj(srvInterface.allocator, VerticaLs);
  }

  void VerticaLs::processFile(const std::string& dir, const char* file_name, Vertica::PartitionWriter &outputWriter) {
    std::string full_path = dir + file_name;
    struct stat stat_buf{};
    int rc = stat(full_path.c_str(), &stat_buf);
    if(rc != 0) {
      return;
    }
    outputWriter.getStringRef(0).copy(full_path);
    if(S_ISREG(stat_buf.st_mode)) {
      outputWriter.getStringRef(1).copy("f");
    } else if (S_ISDIR(stat_buf.st_mode)) {
      outputWriter.getStringRef(1).copy("d");
    } else if (S_ISLNK(stat_buf.st_mode)) {
      outputWriter.getStringRef(1).copy("l");
    } else {
      outputWriter.getStringRef(1).copy("o");
    }
    outputWriter.setTimestamp(2, Vertica::getTimestampTzFromUnixTime(stat_buf.st_ctime));
    outputWriter.setInt(3, stat_buf.st_size);

    outputWriter.next();
  }

  void VerticaLs::processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) {
    UDxUtilities::check_argument_count(inputReader, 1);
    DIR *dp;
    try {
      do {
        const Vertica::VString& path = inputReader.getStringRef(0);
        std::string dir = path.str();
        if(!dir.empty() && dir.back() != '/') {
          dir.append("/");
        }
        dp = opendir(dir.c_str());
        if (dp != nullptr) {
          struct dirent *entry;
          while (true) {
            entry = readdir(dp);
            if(entry == nullptr) {
              break;
            }
            processFile(dir, entry->d_name, outputWriter);
          }
          closedir(dp);
          dp = nullptr;
        } else {
          processFile("", path.str().c_str(), outputWriter);
        }
      } while ( inputReader.next() );
    } catch(const std::exception& e) {
      if(dp != nullptr) {
        closedir(dp);
      }
      vt_report_error(0, "Exception: [%s]", e.what());
    }
  }

  RegisterFactory(VerticaLsFactory);

} // namespaces VerticaUDxUtilities
