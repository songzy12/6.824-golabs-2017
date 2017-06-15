rm log*
for i in {1..1}
do
    go test -run StaticShards -v
    rc=$?
    if [[ $rc != 0 ]]; then exit $rc; fi
    go test -run JoinLeave -v
    rc=$?
    if [[ $rc != 0 ]]; then exit $rc; fi
    go test -run Snapshot -v
    rc=$?
    if [[ $rc != 0 ]]; then exit $rc; fi
    go test -run MissChange -v
    rc=$?
    if [[ $rc != 0 ]]; then exit $rc; fi
    go test -run Concurrent -v
    rc=$?
    if [[ $rc != 0 ]]; then exit $rc; fi
    go test -run Unreliable -v
    rc=$?
    if [[ $rc != 0 ]]; then exit $rc; fi
done
