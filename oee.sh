source .env/bin/activate
faust -A oee worker -l info | tee oee.log

