TODO
====

This document lists the work that needs to be done.

* Support migrations running from a specific major version onwards.
  i.e. If database.version = 1.2.3 and there exists a migration named
  "Release-1.2.3" then start running migrations from there on and just
  register the previous migrations as already applied.

* Support running the tests using
  - rake tasks,
  - the packaged jar file approach
  - improve validation on all the attribute writers

* Add a github pages site that
  - Documents the basic workflow
  - Describes some use cases in detail
  - Adds reference documentation for the configuration
