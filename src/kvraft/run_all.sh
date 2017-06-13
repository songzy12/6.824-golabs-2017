rm log
for i in {1..10}
do
    #go test -run Snapshot -v
    go test -v
    rc=$?;
    echo $rc
    if [[ $rc != 0 ]]; then exit $rc; fi
done
