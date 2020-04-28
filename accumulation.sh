source .env/bin/activate
faust -A accumulation worker -l info | tee accumulation.log