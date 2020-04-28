faust -A accumulation worker -l info | tee accumulation.log &
faust -A availability worker -l info | tee availability.log &
faust -A performance worker -l info | tee performance.log &
faust -A quality worker -l info | tee quality.log &
faust -A oee worker -l info | tee oee.log