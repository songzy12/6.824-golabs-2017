for i in {1..20}
do
    rm log
    go test -run TestFigure8Unreliable2C -v
    rc=$?; 
    echo $rc
    if [[ $rc != 0 ]]; then exit $rc; fi
done
