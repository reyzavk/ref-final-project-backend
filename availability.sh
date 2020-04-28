source .env/bin/activate
faust -A availability worker -l info | tee availability.log
