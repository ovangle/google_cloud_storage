version 0.4.0
=============

breaking changes
    - In fs library, CloudFilesystem renamed to Filesystem
    - fs.dart library now no longer available as separate import (exported with server and client libs)
    - copyTo no longer accepts `StorageObject` as a destination object argument
        - Google cloud no longer supported it

features
    - Testing library (lib/testing/testing_server.dart and lib/testing/testing_client.dart),
        exposes a mock connection object
    - Added `statusCode` getter to `RpcException`
    - Added `range` optional parameter to downloadObject
    - uploadObjectSimple and uploadObjectMultipart requests available on CloudStorageConnection object
    - createBucket now adds 'project' parameter automatically if not provided

bugfixes
    - Reconciled fs library with breaking changes to connection objects
    - copyTo sends an empty content


version 0.3.0+1
===============

No change, but timeout exception occured when attempting to fetch package from pub.
Reuploading

version 0.3.0
=============

Almost complete library rewrite. Almost no code from previous versions valid any longer

version 0.2.0+2
===============

bugfixes
  - storage object handler now (correctly) accepts StreamedResponse
  - upload resumable now posts to correct url
  - correct number of args passed to storage object handler

version 0.2.0+1
===============

bugfixes
  - http connection now uses http.BrowserClient for requests
  - Json list delegates not being pathed properly

version 0.2.0
=============

breaking changes
  - uploadObject now returns a `ResumeToken`. Resume tokens are serializable objects
    which can be used to resume an upload across sessions (without storing the resume data in memory)

bugfixes
  - Retrying a failed request no longer fails with `request already finalised` error

version 0.1.0
=============

Initial version
