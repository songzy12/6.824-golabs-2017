for i in {1..100}
do
    rm log*
    go test -run SnapshotUnreliableRecover$ -v > res
    #go test -run SnapshotRecoverManyClients$ -v > res
    rc=$?;
    if [[ $rc != 0 ]]; then exit $rc; fi
done
