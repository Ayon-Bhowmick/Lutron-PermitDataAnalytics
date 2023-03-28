#!/bin/bash

if [ "$1" = "--ingest" ]; then
    cd ./ingestion
    if [ "$2" = "get" ] || [ -z "$2" ]; then
        echo "Downloading data..."
        if [ ! -d "raw_data" ]; then
            mkdir raw_data
        fi
        python get_data.py
    fi
    if [ "$2" = "strip" ] || [ -z "$2" ]; then
        echo "Stripping data..."
        if [ ! -d "stripped_data" ]; then
            mkdir stripped_data
        fi
        python strip_data.py
    fi
    if [ "$2" = "combine" ] || [ -z "$2" ]; then
        echo "Combining data..."
        if [ ! -d "combined_data" ]; then
            mkdir combined_data
        fi
        python combine_data.py
    fi
    cd ..
elif [ "$1" = "--install" ]; then
    pip install -r requirements.txt
elif [ "$1" = "--reqs" ]; then
    pip install pipreqs
	pipreqs . --force
fi
