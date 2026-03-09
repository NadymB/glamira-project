#!/bin/bash

QUERY='{
    "$in":[
        "view_product_detail",
        "select_product_option",
        "select_product_option_quality",
        "add_to_cart_action",
        "product_detail_recommendation_visible",
        "product_detail_recommendation_noticed",
        "product_view_all_recommend_clicked"
    ]
}'

FIELDS="product_id,viewing_product_id,current_url,referrer_url"

export_chunk () {

    START=$1
    END=$2
    FILE=$3

    mongoexport --db countly --collection summary \
    --query "{ \"time_stamp\": { \"\$gte\":$START, \"\$lt\":$END }, \"collection\": $QUERY }" \
    --fields $FIELDS \
    | python producer_split.py --input - &
}

export_chunk 1585699201 1586394200
export_chunk 1586394200 1587089200
export_chunk 1587089200 1587784200
export_chunk 1587784200 1588479200

wait
echo "Export completed"