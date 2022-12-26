http://nil.csail.mit.edu/6.824/2017/

## Lab 1: MapReduce

http://nil.csail.mit.edu/6.824/2017/labs/lab-1.html

```
bash ./test-mr.sh
```

## Lab 2: Raft

http://nil.csail.mit.edu/6.824/2017/labs/lab-raft.html

```
cd src/raft
go test
```

## Lab 3: Fault-tolerant Key/Value Service

http://nil.csail.mit.edu/6.824/2017/labs/lab-kvraft.html

```
cd src/kvraft
go test
```

## Lab 4: Sharded Key/Value Service

http://nil.csail.mit.edu/6.824/2017/labs/lab-shard.html

```
cd src/shardmaster
go test

cd src/shardkv
go test
```

## Appendix

### GOROOT

> GOROOT is for compiler and tools that come from go installation and is used to find the standard libraries. It should always be set to the installation directory.

```
$ go env GOROOT
/usr/local/go
```

### GOPATH

https://www.geeksforgeeks.org/golang-gopath-and-goroot/

> GOPATH, also called the _workspace directory_, is the directory where the Go code belongs. It is implemented by and documented in the go/build package and is used to resolve import statements. The _go get_ tool downloads packages to the first directory in GOPATH. If the environment variable is unset, GOPATH defaults to a subdirectory named “go” in the user’s home directory.

```
$ go env GOPATH
/home/songzy/go
```

> GOPATH contains 3 directories under it and each directory under it has specific functions:
>
> - src: It holds source code. The path below this directory determines the import path or the executable name.
> - pkg: It holds installed package objects. Each target operating system and architecture pair has its own subdirectory of pkg.
> - bin: It holds compiled commands. Every command is named for its source directory.

> When using modules in Go, the GOPATH is no longer used to determine imports. However, it is still used to store downloaded source code in pkg and compiled commands bin.

### go.mod

https://go.dev/blog/using-go-modules

> Starting in Go 1.13 (2019), module mode will be the default for all development.

> The go.mod file only appears in the root of the module. Packages in subdirectories have import paths consisting of the module path plus the path to the subdirectory. 
> For example, if we created a subdirectory world, we would not need to (nor want to) run go mod init there. The package would automatically be recognized as part of the example.com/hello module, with import path example.com/hello/world.

> A second go test command will not repeat this work, since the go.mod is now up-to-date and the downloaded modules are cached locally (in $GOPATH/pkg/mod).

> With Go modules, versions are referenced with semantic version tags. A semantic version has three parts: major, minor, and patch. For example, for v0.1.2, the major version is 0, the minor version is 1, and the patch version is 2.

> The go mod tidy command cleans up these unused dependencies.

### go.work

https://github.com/golang/tools/blob/master/gopls/doc/workspace.md
