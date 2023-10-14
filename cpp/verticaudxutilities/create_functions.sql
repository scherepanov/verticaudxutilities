\set ON_ERROR_STOP on
create or replace library VerticaUDxUtilities AS :libfile;

create or replace transform function VerticaLs               as language 'C++' name 'VerticaLsFactory'               library VerticaUDxUtilities :fencing fenced;
create or replace transform function Seq                     as language 'C++' name 'SeqFactory'                     library VerticaUDxUtilities :fencing fenced;
create or replace transform function Args                    as language 'C++' name 'ArgsFactory'                    library VerticaUDxUtilities :fencing fenced;
create or replace transform function MPArgs                  as language 'C++' name 'MPArgsFactory'                  library VerticaUDxUtilities :fencing fenced;
create or replace transform function Csv                     as language 'C++' name 'CsvFactory'                     library VerticaUDxUtilities :fencing fenced;
create or replace transform function RowCount                as language 'C++' name 'RowCountFactory'                library VerticaUDxUtilities :fencing fenced;
create or replace           function sql_digest              as language 'C++' name 'SQLDigestFactory'               library VerticaUDxUtilities          fenced;
create or replace transform function vertica_log             as language 'C++' name 'VerticaLogFactory'              library VerticaUDxUtilities          fenced;
create or replace transform function vertica_log_size        as language 'C++' name 'VerticaLogSizeFactory'          library VerticaUDxUtilities          fenced;

create or replace           function To_Int                  as language 'C++' name 'ToIntFactory'                   library VerticaUDxUtilities :fencing fenced;
create or replace           function To_Numeric              as language 'C++' name 'ToNumericFactory'               library VerticaUDxUtilities :fencing fenced;
create or replace           function ToDate                  as language 'C++' name 'ToDateFactory'                  library VerticaUDxUtilities :fencing fenced;
create or replace           function ToTime                  as language 'C++' name 'ToTimeFactory'                  library VerticaUDxUtilities :fencing fenced;
create or replace           function ToTimeTZ                as language 'C++' name 'ToTimeTZFactory'                library VerticaUDxUtilities :fencing fenced;
create or replace           function ToTimestamp             as language 'C++' name 'ToTimestampFactory'             library VerticaUDxUtilities :fencing fenced;
create or replace           function ToTimestampTZ           as language 'C++' name 'ToTimestampTZFactory'           library VerticaUDxUtilities :fencing fenced;
create or replace           function ToInterval              as language 'C++' name 'ToIntervalFactory'              library VerticaUDxUtilities :fencing fenced;
create or replace           function ToIntervalYM            as language 'C++' name 'ToIntervalYMFactory'            library VerticaUDxUtilities :fencing fenced;

create or replace           function UnixDaysToDate          as language 'C++' name 'UnixDaysToDateFactory'          library VerticaUDxUtilities :fencing fenced;
create or replace           function DateToUnixDays          as language 'C++' name 'DateToUnixDaysFactory'          library VerticaUDxUtilities :fencing fenced;
create or replace           function UnixMicrosToTimestamp   as language 'C++' name 'UnixMicrosToTimestampFactory'   library VerticaUDxUtilities :fencing fenced;
create or replace           function TimestampToUnixMicros   as language 'C++' name 'TimestampToUnixMicrosFactory'   library VerticaUDxUtilities :fencing fenced;
create or replace           function MidnightMicrosToTime    as language 'C++' name 'MidnightMicrosToTimeFactory'    library VerticaUDxUtilities :fencing fenced;
create or replace           function TimeToMidnightMicros    as language 'C++' name 'TimeToMidnightMicrosFactory'    library VerticaUDxUtilities :fencing fenced;
create or replace           function MidnightMicrosToTimeTz  as language 'C++' name 'MidnightMicrosToTimeTzFactory'  library VerticaUDxUtilities :fencing fenced;
create or replace           function TimeTzToMidnightMicros  as language 'C++' name 'TimeTzToMidnightMicrosFactory'  library VerticaUDxUtilities :fencing fenced;

create or replace           function UnixMicrosToDate        as language 'C++' name 'UnixMicrosToDateFactory'        library VerticaUDxUtilities :fencing fenced;

create or replace           function UnixNanosToTimestamp    as language 'C++' name 'UnixNanosToTimestampFactory'    library VerticaUDxUtilities :fencing fenced;
create or replace           function UnixNanosToTimestampTz  as language 'C++' name 'UnixNanosToTimestampTzFactory'  library VerticaUDxUtilities :fencing fenced;
create or replace           function UnixMicrosToTimestampTz as language 'C++' name 'UnixMicrosToTimestampTzFactory' library VerticaUDxUtilities :fencing fenced;

create or replace           function MicrosSinceEpoch        as language 'C++' name 'MicrosSinceEpochFactory'        library VerticaUDxUtilities :fencing fenced;
create or replace           function NanosSinceEpoch         as language 'C++' name 'NanosSinceEpochFactory'         library VerticaUDxUtilities :fencing fenced;
create or replace           function Midnight_Nanos          as language 'C++' name 'MidnightNanosFactory'           library VerticaUDxUtilities :fencing fenced;

create or replace           function BinToChar               as language 'C++' name 'BinaryToCharFactory'            library VerticaUDxUtilities :fencing fenced;
create or replace           function CharToBin               as language 'C++' name 'CharToBinaryFactory'            library VerticaUDxUtilities :fencing fenced;
create or replace           function Base36ToInt             as language 'C++' name 'Base36ToIntFactory'             library VerticaUDxUtilities :fencing fenced;
create or replace           function IntToBase36             as language 'C++' name 'IntToBase36Factory'             library VerticaUDxUtilities :fencing fenced;

create or replace           function lina_interpolate        as language 'C++' name 'ArrayLinearInterpolateFactory'  library VerticaUDxUtilities :fencing fenced;
create or replace           function lina_arr_make           as language 'C++' name 'ArrayMakeFactory'               library VerticaUDxUtilities :fencing fenced;
create or replace           function lina_arr_neg            as language 'C++' name 'ArrayNegFactory'                library VerticaUDxUtilities :fencing fenced;

create or replace           function lina_arr_add            as language 'C++' name 'ArrayAddFactory'                library VerticaUDxUtilities :fencing fenced;
create or replace           function lina_arr_sub            as language 'C++' name 'ArraySubFactory'                library VerticaUDxUtilities :fencing fenced;
create or replace           function lina_arr_mul            as language 'C++' name 'ArrayMulFactory'                library VerticaUDxUtilities :fencing fenced;
create or replace           function lina_arr_div            as language 'C++' name 'ArrayDivFactory'                library VerticaUDxUtilities :fencing fenced;

create or replace           function lina_arr_add            as language 'C++' name 'ArrayAdd0Factory'               library VerticaUDxUtilities :fencing fenced;
create or replace           function lina_arr_sub            as language 'C++' name 'ArraySub0Factory'               library VerticaUDxUtilities :fencing fenced;
create or replace           function lina_arr_mul            as language 'C++' name 'ArrayMul0Factory'               library VerticaUDxUtilities :fencing fenced;
create or replace           function lina_arr_div            as language 'C++' name 'ArrayDiv0Factory'               library VerticaUDxUtilities :fencing fenced;

create or replace           function lina_arr_add            as language 'C++' name 'ArrayAdd1Factory'               library VerticaUDxUtilities :fencing fenced;
create or replace           function lina_arr_sub            as language 'C++' name 'ArraySub1Factory'               library VerticaUDxUtilities :fencing fenced;
create or replace           function lina_arr_mul            as language 'C++' name 'ArrayMul1Factory'               library VerticaUDxUtilities :fencing fenced;
create or replace           function lina_arr_div            as language 'C++' name 'ArrayDiv1Factory'               library VerticaUDxUtilities :fencing fenced;

grant execute on all functions in schema public to public;
\set ON_ERROR_STOP off

\i /tmp/test_queries.sql

