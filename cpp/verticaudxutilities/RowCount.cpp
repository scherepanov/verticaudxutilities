#include "RowCount.h"
#include <string>

namespace VerticaUDxUtilities {

void RowCountFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addAny();
  returnType.addVarchar();
  returnType.addInt();
}

void RowCountFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  outputTypes.addVarchar(64, "node_name");
  outputTypes.addInt("row_count");
}

Vertica::TransformFunction * RowCountFactory::createTransformFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, RowCount);
}

RegisterFactory(RowCountFactory);

void RowCount::processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) {
  long row_count = 0;
  do {
    row_count++;
  } while ( inputReader.next() );
  outputWriter.getStringRef(0).copy(srvInterface.getCurrentNodeName().c_str());
  outputWriter.setInt(1, row_count);
  outputWriter.next();
}

} // namespaces VerticaUDxUtilities
