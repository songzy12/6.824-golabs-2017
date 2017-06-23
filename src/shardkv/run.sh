for i in {1..100}
do
    #rm log*
    #go test -run StaticShards -v
    #rc=$?
    #if [[ $rc != 0 ]]; then exit $rc; fi
    #rm log*
    #go test -run JoinLeave -v
    #rc=$?
    #if [[ $rc != 0 ]]; then exit $rc; fi
    #rm log*
    #go test -run Snapshot -v
    #rc=$?
    #if [[ $rc != 0 ]]; then exit $rc; fi
    #rm log*
    #go test -run MissChange -v
    #rc=$?
    #if [[ $rc != 0 ]]; then exit $rc; fi
    #rm log*
    #go test -run Concurrent2 -v
    #rc=$?
    #if [[ $rc != 0 ]]; then exit $rc; fi
    #rm log*
    #go test -run Concurrent1 -v
    #rc=$?
    #if [[ $rc != 0 ]]; then exit $rc; fi
    rm log*
    go test -run Unreliable2 -v
    rc=$?
    if [[ $rc != 0 ]]; then exit $rc; fi
done
