
<a name="v0.1.2"></a>
## [v0.1.2](https://github.com/jeffrom/logd/compare/v0.1.1...v0.1.2) (2019-05-05)

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
