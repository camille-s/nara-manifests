#!/usr/bin/env bash
ids=$1
limit=$2
dryrun=$3
# read rows of ids file, call python script on each id
# if dryrun is set, add no_download flag
if [ "$dryrun" == "dryrun" ]; then
    flag=" -n"
else 
    flag=""
fi
while IFS= read -r row; do 
    # split by comma
    name=$(echo "$row" | cut -d',' -f1)
    id=$(echo "$row" | cut -d',' -f2)
    echo ""
    echo "Processing series '$name' with id $id"
    python ./fetch_records.py --id "$id" --limit "$limit""$flag"
done < "$ids"