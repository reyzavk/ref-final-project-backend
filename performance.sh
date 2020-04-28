source .env/bin/activate
faust -A performance worker -l info | tee performance.log