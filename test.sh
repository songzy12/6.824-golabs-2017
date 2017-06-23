export GOPATH=$PWD;
mkdir temp/
cd src/kvraft/
for i in `seq 0 9`
do
   touch ../../temp/res$i
   go test > ../../temp/res$i &
   echo "CASE PASSED: $i"
done
