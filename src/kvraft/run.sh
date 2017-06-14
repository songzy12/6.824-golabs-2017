for i in {1..20}
do
    rm log
    go test -run TestSnapshotUnreliable$ -v
    rc=$?;
    if [[ $rc != 0 ]]; then exit $rc; fi
done
