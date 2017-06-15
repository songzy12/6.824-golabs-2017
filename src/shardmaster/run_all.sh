rm log*
for i in {1..1}
do
    go test -run Basic -v
    rc=$?
    if [[ $rc != 0 ]]; then exit $rc; fi
    go test -run Multi -v
    rc=$?
    if [[ $rc != 0 ]]; then exit $rc; fi
done
