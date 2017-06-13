rm log
for i in {1..20}
do
    go test -run 2A -v
    rc=$?;
    if [[ $rc != 0 ]]; then exit $rc; fi 
    go test -run 2B -v
    rc=$?;
    if [[ $rc != 0 ]]; then exit $rc; fi 
    go test -run 2C -v
    rc=$?;
    if [[ $rc != 0 ]]; then exit $rc; fi 
done
