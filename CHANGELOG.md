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