
<a name="v0.3.0"></a>
## [v0.3.0](https://github.com/jeffrom/logd/compare/v0.2.1...v0.3.0) (2020-02-21)

### Chore

* misc build cleanups
* update depdendencies
* update dep management in ci a bit ([#4](https://github.com/jeffrom/logd/issues/4))
* update to go 1.13 ([#3](https://github.com/jeffrom/logd/issues/3))
* **ci:** last fix for branch detection (i hope)
* **ci:** fix branch-based benchark comparison

### Doc

* add some comparison data between go 1.12 and 1.13

### Feat

* limit max messages returned by read, tail

### Fix

* reconnect in tests would hit localhost 1774 ([#7](https://github.com/jeffrom/logd/issues/7))


<a name="v0.2.1"></a>
## [v0.2.1](https://github.com/jeffrom/logd/compare/v0.2.0...v0.2.1) (2019-05-20)

### Chore

* update dependencies


<a name="v0.2.0"></a>
## [v0.2.0](https://github.com/jeffrom/logd/compare/v0.1.11...v0.2.0) (2019-05-20)

### Chore

* replace WORK.md with ROADMAP.md
* allow setting -count flag in bench tasks
* document configuration
* tool to check outdated dependencies
* fix docker image tags
* remove TODO.wiki
* tweak release commit message template
* **client:** set FileStatePuller as not implemented

### Feat

* topic limits

### Fix

* **client:** set backlogC when calling WithBacklog

### Style

* **cli:** change version format

### Test

* **events:** multiple batch read request response


<a name="v0.1.11"></a>
## [v0.1.11](https://github.com/jeffrom/logd/compare/v0.1.10...v0.1.11) (2019-05-05)

### Chore

* change semver script
* include changelog, readme in release


<a name="v0.1.10"></a>
## [v0.1.10](https://github.com/jeffrom/logd/compare/v0.1.9...v0.1.10) (2019-05-05)

### Chore

* travis should only release from go 1.12


<a name="v0.1.9"></a>
## [v0.1.9](https://github.com/jeffrom/logd/compare/v0.1.8...v0.1.9) (2019-05-05)


<a name="v0.1.8"></a>
## [v0.1.8](https://github.com/jeffrom/logd/compare/v0.1.7...v0.1.8) (2019-05-05)


<a name="v0.1.7"></a>
## [v0.1.7](https://github.com/jeffrom/logd/compare/v0.1.6...v0.1.7) (2019-05-05)

### Chore

* don't use snap yet


<a name="v0.1.6"></a>
## [v0.1.6](https://github.com/jeffrom/logd/compare/v0.1.5...v0.1.6) (2019-05-05)


<a name="v0.1.5"></a>
## [v0.1.5](https://github.com/jeffrom/logd/compare/v0.1.4...v0.1.5) (2019-05-05)


<a name="v0.1.4"></a>
## [v0.1.4](https://github.com/jeffrom/logd/compare/v0.1.3...v0.1.4) (2019-05-05)


<a name="v0.1.3"></a>
## [v0.1.3](https://github.com/jeffrom/logd/compare/v0.1.2...v0.1.3) (2019-05-05)


<a name="v0.1.2"></a>
## [v0.1.2](https://github.com/jeffrom/logd/compare/v0.1.0-494184431...v0.1.2) (2019-05-05)


<a name="v0.1.0-494184431"></a>
## [v0.1.0-494184431](https://github.com/jeffrom/logd/compare/v0.1.1...v0.1.0-494184431) (2019-05-05)

### Chore

* push images to docker, releases to github with travis
* set up goreleaser to push docker images


<a name="v0.1.1"></a>
## [v0.1.1](https://github.com/jeffrom/logd/compare/v0.1.0...v0.1.1) (2019-05-04)

### Chore

* fixing bump version script
* bump version script


<a name="v0.1.0"></a>
## v0.1.0 (2019-01-21)

### Logger

* use a sync.Pool to manage file handles for Range

### Optimization

* cache batch scanner for reading

### Server

* cache response, clientresponse with reqPool

### Wip

* http request to /log interacts with logd

