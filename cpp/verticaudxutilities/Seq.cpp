#include <cmath>
#include "Seq.h"

namespace VerticaUDxUtilities {

void SeqFactory::getPrototype(Vertica::ServerInterface &srvInterface, Vertica::ColumnTypes &argTypes, Vertica::ColumnTypes &returnType){
  argTypes.addAny();
  returnType.addAny();
}

void SeqFactory::getReturnType(Vertica::ServerInterface &srvInterface, const Vertica::SizedColumnTypes &inputTypes, Vertica::SizedColumnTypes &outputTypes){
  std::vector<size_t> arg_cols;
  inputTypes.getArgumentColumns(arg_cols);
  if (arg_cols.size() < 2 || arg_cols.size() > 3) {
    vt_report_error(1, "VerticaUDxUtilities function Seq provided with %lu arguments, require 2 or 3  arguments - begin and end of sequence, and optional increment", arg_cols.size());
  }
  const Vertica::VerticaType& seq_type = inputTypes.getColumnType(arg_cols[0]);
  const Vertica::VerticaType& last_type = inputTypes.getColumnType(arg_cols[1]);

  bool debug = srvInterface.getParamReader().containsParameter("debug");

  if(debug) srvInterface.log("Seq types first %s last %s", seq_type.getTypeStr(), last_type.getTypeStr());

  if(! (seq_type.isInt() || seq_type.isFloat() || seq_type.isDate() || seq_type.isTimestamp() || seq_type.isTimestampTz() || seq_type.isTime() )) {
    vt_report_error(3, "VerticaUDxUtilities function Seq unsupported type of arguments");
  }
  if(debug) srvInterface.log("getReturnType");
  if(arg_cols.size() == 3) {
    const Vertica::VerticaType& incr_type = inputTypes.getColumnType(arg_cols[2]);
    if(debug) srvInterface.log("Incr type %s", incr_type.getTypeStr());
    if (seq_type.isInt() && !incr_type.isInt()) {
      vt_report_error(4, "VerticaUDxUtilities function Seq int argument requre int increment");
    } else if (seq_type.isFloat() && ! incr_type.isFloat()) {
      vt_report_error(6, "VerticaUDxUtilities function Seq float argument requre float, int or numeric increment");
    } else if (seq_type.isDate() && ! (incr_type.isInt())) {
      vt_report_error(7, "VerticaUDxUtilities function Seq date argument requre int or intervalYM increment");
    } else if ((seq_type.isTimestamp() || seq_type.isTimestampTz() || seq_type.isTime()) && ! (incr_type.isInt() || incr_type.isInterval())) {
      vt_report_error(8, "VerticaUDxUtilities function Seq timestamp, timestampTz or time argument requre int, numeric or interval increment");
    }
  }

  outputTypes.addArg(seq_type, "seq");

}

void SeqFactory::getParameterType(Vertica::ServerInterface &srvInterface, Vertica::SizedColumnTypes &parameterTypes) {
  parameterTypes.addVarchar(32, "debug");
}

Vertica::TransformFunction * SeqFactory::createTransformFunction(Vertica::ServerInterface &srvInterface){
  return vt_createFuncObj(srvInterface.allocator, Seq);
}

void Seq::processPartition(Vertica::ServerInterface &srvInterface, Vertica::PartitionReader &inputReader, Vertica::PartitionWriter &outputWriter) {
  bool debug = srvInterface.getParamReader().containsParameter("debug");
  if(debug) srvInterface.log("Process partition");
  const Vertica::SizedColumnTypes meta = inputReader.getTypeMetaData();
  std::vector<size_t> arg_cols;
  meta.getArgumentColumns(arg_cols);
  bool incr_present = arg_cols.size() == 3;
  const Vertica::VerticaType& seq_type = meta.getColumnType(arg_cols[0]);

  try {
    do {
      if(debug) srvInterface.log("Input argument loop");
      if(seq_type.isInt() || seq_type.isTimestamp() || seq_type.isTimestampTz() || seq_type.isTime() || seq_type.isDate()) {
        if(debug) srvInterface.log("Input arguments int, timestamp, TimestampTz, time or date");
        Vertica::vint first = inputReader.getIntRef(arg_cols[0]);
        Vertica::vint last = inputReader.getIntRef(arg_cols[1]);
        Vertica::vint incr;
        if(incr_present) {
          incr = inputReader.getIntRef(arg_cols[2]);
        } else {
          incr = (first < last ? 1 : -1 );
          if(!seq_type.isInt() && !seq_type.isDate() && !seq_type.isFloat()) {
            incr *= 1000000;
          }
        }
        if(incr == 0) {
          vt_report_error( 10, "Zero increment" );
        }
        if( (first < last && incr < 0) || (first > last && incr > 0) ) {
          incr = -incr;
        }
        if(debug) srvInterface.log("Seq first %lli last %lli incr %lli", first, last, incr);
        Vertica::vint lp = first;
        do {
          if(seq_type.isInt()) {
            outputWriter.setInt(0, lp);
          } else if (seq_type.isTimestamp()) {
            outputWriter.setTimestamp(0, lp);
          } else if (seq_type.isTimestampTz()) {
            outputWriter.setTimestampTz(0, lp);
          } else if (seq_type.isTime()) {
            outputWriter.setTime(0, lp);
          } else if (seq_type.isDate()) {
            outputWriter.setDate(0, lp);
          }
          //if(debug) srvInterface.log("Output seq %lli", lp);
          outputWriter.next();
          lp += incr;
        } while ((incr > 0 && lp <= last) || (incr < 0 && lp >= last));
      } else if (seq_type.isFloat()) {
        if(debug) srvInterface.log("Input arguments float");
        Vertica::vfloat first = inputReader.getFloatRef(arg_cols[0]);
        Vertica::vfloat last = inputReader.getFloatRef(arg_cols[1]);
        Vertica::vfloat incr;
        if(incr_present) {
          incr = inputReader.getFloatRef(arg_cols[2]);
        } else {
          incr = (first < last ? 1 : -1 );
        }
        if(incr == 0)
          vt_report_error(10, "Zero increment");
        if( (first < last && incr < 0) || (first > last && incr > 0) ) {
          incr = -incr;
        }
        if(debug) srvInterface.log("Seq first %f last %f incr %f", first, last, incr);
        Vertica::vfloat lp =first;
        do {
          outputWriter.setFloat(0, static_cast<double>(lp));
          //if(debug) srvInterface.log("Output seq %f", lp);
          outputWriter.next();
          lp += incr;
        } while ((incr > 0 && lp <= last) || (incr < 0 && lp >= last));

      } else {
        if(debug) srvInterface.log("Unsupported argument type");
        vt_report_error(20,"Unsupported argument type");
      }
    } while ( inputReader.next() );
  } catch(const std::exception& e) {
    vt_report_error(0, "Exception: [%s]", e.what());
  }
}

RegisterFactory(SeqFactory);

} // namespaces VerticaUDxUtilities

