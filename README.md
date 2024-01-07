# Vertica UDx Utilities

UDx utilities is a c++ library containing helper functions for working with Vertica.
It also contains few functions that are missed from Vertica for various reasons.

## Prerequisites

Library registration claims UDx SDK v 11.1.1, you can build and run starting with Vertica v 9.3.
Library verified to work up to UDx SDK v 12.0.2-2.
You will need to edit library registration and put your Vertica version in cpp/verticaudxutilities/UDxUtilities.cpp.

There is no indications that you will not be able to compile and use library with version 12 and above.

Library compiles with g++ v 10.3 and above, with c++17.
No need to use docker, most likely it will lower your compiler version.

Library linked with static libstdc++, allowing you to use recent version of build server/desktop, and does not depend on OS version on Vertica cluster nodes.
Vertica cluster nodes typically very outdated on OS version, static libstdc++ nicely decouples Vertica ndoes OS version from OS version and compiler version on build box.

Library with statically linked libstdc++ works well in fenced and unfenced mode, it is recommended mode.

It is recommended to use recent OS version for build server/desktop, with most recent g++ compiler. 

It is recommended to raise your c++ to c++20 if you install corresponding g++ compiler version.

If you want, you can lower c++ standard down to c++11, but you will need to do some work on tweaking c++ code.

## Install

Initially you need to run ./build.sh in root dir of repo.

To deploy, you need to run cpp/verticaudxutilities/deploy.sh. It accepts single parameter - node name, one of ndoes in cluster.

You need to have passwordless ssh into dbadmin on one of nodes in Vertica cluster.

Alternatively, you will need to enter OS dbadmin password 3 times per deploy.

Deploy is relying on Vertica recommended configuration of passwordless login into database dbadmin account from any node in cluster, as described in Vertica install guide.

After deployment, it is recommended to create table vertica_dual by running script cpp/verticaudxutilities/vertica_dual.sql.
It is used with Vertica log viewer functions.

Deployment creates all functions in schema public, and give grants to execute to public.

While for most functions public grants are correct, you may consider limiting access to for example VerticaLog, as it gives users full read-only access to vertica and UDx logs.

## Functionality

[Date Time Conversions](docs/DATE_TIME_CONVERSIONS.md)

For vaious reasons, Vertica is missing few conversion functions. 

For example, there is no conversion betweeen VARBINARY and VARCHAR.

[CSV](docs/CSV.md)

Read file on node in Vertica cluster and present result as table with string columns. 
Similar to external table, with CSV function having advantage of not needed to define
external tables for each file.

[SEQ](docs/SEQ.md)

Generates sequence of dates, timestamps, timestamptz, times, integers, floats.

[ROW COUNT](docs/ROW_COUNT.md)

Benchmarking function, forces Vertica to retrieve whole resultset with all columns, without sending to client.

[ARGS](docs/ARGS.md)

Arguments passing function, helpful in tweaking UDx parallel execution. 

[UDx Parallelization with help of args()](docs/UDX_PARALLELIZATION.md)

Explanation how you can control UDx parallelization with ARGS() function.

[SQL_DIGEST](docs/SQL_DIGEST.md)

SQL DIGEST for monitoring Vertica SQL performance

[VERTICA LOG](docs/VERTICA_LOG.md)

Vertica log viewer through SQL.

[VERTICA LS](docs/VERTICA_LS.md)

Listing directories on Vertica nodes through SQL.

[BASE36 TO INT](docs/BASE36_CONVESRSIONS.md)

Contain two functions - InToBase36 and Base36ToInt.

[Dynamic Python](docs/DynPython.md)

Dynamic python execution in Vertica

[Python VerticaStatsTweaking](docs/VerticaStatTweaking.md)

Python for tweaking database stats - extend date columns stats a week ahead, to avoid PREDICATE OUT OF RANGE warning

[Python VerticaTableRedesign](docs/VerticaTableRedesign.md)

Python for automated Vertica table optimisation - create tables with sample data, with new order/segmentation, optimised encodings

## License
Library released under MIT license

