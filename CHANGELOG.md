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
