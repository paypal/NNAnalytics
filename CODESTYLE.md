# Project Codestyle

## General Standard

NameNode Analytics follows the Google Java Styleguide.  
We recommend you use IntelliJ IDEA and install the Checkstyle plugin.  
Once installed please set-up your plugin to follow the checkstyle underneath `config/checkstyle/checkstyle.xml`.  
Documentation on how to do so can be found [here][guide].

## Scope

This codestyle is to be applied across all non-binary files within the `src/main` and `src/test` directories.

## Enforcement

The codestyle is enforced by running `./gradlew checkstyleMain checkstyleTest`.  
Changes which add additional warnings are subject to peer review and are discouraged.  
Any codestyle errors MUST be fixed prior to approving and/or committing.

## Attribution

This codestyle standard is adapted from [Checkstyle][homepage], revision 80365b80228157a07cad060cfd30087487f4b6f9, available [here][version].

[guide]: http://www.practicesofmastery.com/post/intellij-checkstyle-google-java-style-guide/
[homepage]: https://github.com/checkstyle/checkstyle
[version]: https://github.com/checkstyle/checkstyle/blob/80365b80228157a07cad060cfd30087487f4b6f9/src/main/resources/google_checks.xml
