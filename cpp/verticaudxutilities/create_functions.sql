DROP LIBRARY VerticaUDxUtilities CASCADE;

\set ON_ERROR_STOP on
CREATE LIBRARY VerticaUDxUtilities AS :libfile;

create transform function VerticaLs               as language 'C++' name 'VerticaLsFactory'               library VerticaUDxUtilities :fencing fenced;
create transform function Seq                     as language 'C++' name 'SeqFactory'                     library VerticaUDxUtilities :fencing fenced;
create transform function Args                    as language 'C++' name 'ArgsFactory'                    library VerticaUDxUtilities :fencing fenced;
create transform function MPArgs                  as language 'C++' name 'MPArgsFactory'                  library VerticaUDxUtilities :fencing fenced;
create transform function Csv                     as language 'C++' name 'CsvFactory'                     library VerticaUDxUtilities :fencing fenced;
create transform function RowCount                as language 'C++' name 'RowCountFactory'                library VerticaUDxUtilities :fencing fenced;
create           function To_Int                  as language 'C++' name 'ToIntFactory'                   library VerticaUDxUtilities :fencing fenced;
create           function To_Numeric              as language 'C++' name 'ToNumericFactory'               library VerticaUDxUtilities :fencing fenced;
create           function ToDate                  as language 'C++' name 'ToDateFactory'                  library VerticaUDxUtilities :fencing fenced;
create           function ToTime                  as language 'C++' name 'ToTimeFactory'                  library VerticaUDxUtilities :fencing fenced;
create           function ToTimeTZ                as language 'C++' name 'ToTimeTZFactory'                library VerticaUDxUtilities :fencing fenced;
create           function ToTimestamp             as language 'C++' name 'ToTimestampFactory'             library VerticaUDxUtilities :fencing fenced;
create           function ToTimestampTZ           as language 'C++' name 'ToTimestampTZFactory'           library VerticaUDxUtilities :fencing fenced;
create           function BinToChar               as language 'C++' name 'BinaryToCharFactory'            library VerticaUDxUtilities :fencing fenced;
create           function CharToBin               as language 'C++' name 'CharToBinaryFactory'            library VerticaUDxUtilities :fencing fenced;
create           function Base36ToInt             as language 'C++' name 'Base36ToIntFactory'             library VerticaUDxUtilities :fencing fenced;
create           function IntToBase36             as language 'C++' name 'IntToBase36Factory'             library VerticaUDxUtilities :fencing fenced;
create           function UnixNanosToTimestamp    as language 'C++' name 'UnixNanosToTimestampFactory'    library VerticaUDxUtilities :fencing fenced;
create           function UnixNanosToTimestampTz  as language 'C++' name 'UnixNanosToTimestampTzFactory'  library VerticaUDxUtilities :fencing fenced;
create           function UnixMicrosToTimestamp   as language 'C++' name 'UnixMicrosToTimestampFactory'   library VerticaUDxUtilities :fencing fenced;
create           function UnixMicrosToTimestampTz as language 'C++' name 'UnixMicrosToTimestampTzFactory' library VerticaUDxUtilities :fencing fenced;
create           function Midnight_Nanos          as language 'C++' name 'MidnightNanosFactory'           library VerticaUDxUtilities :fencing fenced;
create           function UnixMicrosToDate        as language 'C++' name 'UnixMicrosToDateFactory'        library VerticaUDxUtilities :fencing fenced;
create           function UnixDaysToDate          as language 'C++' name 'UnixDaysToDateFactory'          library VerticaUDxUtilities :fencing fenced;
create           function MicrosSinceEpoch        as language 'C++' name 'MicrosSinceEpochFactory'        library VerticaUDxUtilities :fencing fenced;
create           function NanosSinceEpoch         as language 'C++' name 'NanosSinceEpochFactory'         library VerticaUDxUtilities :fencing fenced;
create           function sql_digest              as language 'C++' name 'SQLDigestFactory'               library VerticaUDxUtilities          fenced;
create transform function vertica_log             as language 'C++' name 'VerticaLogFactory'              library VerticaUDxUtilities          fenced;
create transform function vertica_log_size        as language 'C++' name 'VerticaLogSizeFactory'          library VerticaUDxUtilities          fenced;

create           function lina_interpolate        as language 'C++' name 'ArrayLinearInterpolateFactory'  library VerticaUDxUtilities :fencing fenced;
create           function lina_arr_make           as language 'C++' name 'ArrayMakeFactory'               library VerticaUDxUtilities :fencing fenced;
create           function lina_arr_neg            as language 'C++' name 'ArrayNegFactory'                library VerticaUDxUtilities :fencing fenced;

create           function lina_arr_add            as language 'C++' name 'ArrayAddFactory'                library VerticaUDxUtilities :fencing fenced;
create           function lina_arr_sub            as language 'C++' name 'ArraySubFactory'                library VerticaUDxUtilities :fencing fenced;
create           function lina_arr_mul            as language 'C++' name 'ArrayMulFactory'                library VerticaUDxUtilities :fencing fenced;
create           function lina_arr_div            as language 'C++' name 'ArrayDivFactory'                library VerticaUDxUtilities :fencing fenced;

create           function lina_arr_add            as language 'C++' name 'ArrayAdd0Factory'               library VerticaUDxUtilities :fencing fenced;
create           function lina_arr_sub            as language 'C++' name 'ArraySub0Factory'               library VerticaUDxUtilities :fencing fenced;
create           function lina_arr_mul            as language 'C++' name 'ArrayMul0Factory'               library VerticaUDxUtilities :fencing fenced;
create           function lina_arr_div            as language 'C++' name 'ArrayDiv0Factory'               library VerticaUDxUtilities :fencing fenced;

create           function lina_arr_add            as language 'C++' name 'ArrayAdd1Factory'               library VerticaUDxUtilities :fencing fenced;
create           function lina_arr_sub            as language 'C++' name 'ArraySub1Factory'               library VerticaUDxUtilities :fencing fenced;
create           function lina_arr_mul            as language 'C++' name 'ArrayMul1Factory'               library VerticaUDxUtilities :fencing fenced;
create           function lina_arr_div            as language 'C++' name 'ArrayDiv1Factory'               library VerticaUDxUtilities :fencing fenced;

grant execute on all functions in schema public to public;
\set ON_ERROR_STOP off

\i /tmp/test_queries.sql

