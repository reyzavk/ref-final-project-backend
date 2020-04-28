source .env/bin/activate
faust -A quality worker -l info | tee quality.log
