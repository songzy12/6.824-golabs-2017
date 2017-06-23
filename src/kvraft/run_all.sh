for i in {1..10}
do
    #go test -run Snapshot -v
    rm log*
    go test -v > res
    rc=$?;
    echo $rc
    if [[ $rc != 0 ]]; then exit $rc; fi
done
