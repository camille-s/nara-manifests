#!/usr/bin/env bash
ids=$1
limit=$2
dryrun=$3
# read rows of ids file, call python script on each id
# if dryrun is set, add no_download flag
while IFS= read -r row; do
    # split by comma
    name=$(echo "$row" | cut -d',' -f1)
    id=$(echo "$row" | cut -d',' -f2)
    # only run if both name and id are not blank and not commented

    echo ""
    echo "Processing series '$name' with id $id"
    if [ "$dryrun" == "dryrun" ]; then
        python ./fetch_records.py --id "$id" --limit "$limit" -n
    else
        python ./fetch_records.py --id "$id" --limit "$limit"
    fi
    # done < "$ids"
done < <(grep -vE "(^#|^\s*?$)" "$ids")
