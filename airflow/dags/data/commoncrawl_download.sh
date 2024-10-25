if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <crawl> <segments_list>"
    exit 1
fi

base_path=https://data.commoncrawl.org
crawl=$1
# crawl=CC-MAIN-2024-38

segments=$2
n_file_per_segment=${3:-90000}

processed_warc_file=processed_warc_file.txt
# Check if the file exists, if not create an empty one
if [ ! -f "$processed_warc_file" ]; then
    touch "$processed_warc_file"
fi

mkdir warc
cd warc

# Download the CC warc paths file
curl -s "$base_path/crawl-data/$crawl/warc.paths.gz" -o warc_paths.gz

# Unzip the file to get the list of files
gunzip -f warc_paths.gz

# Initialize a counter for the files
counter=0

# Loop through each segment
for segment in $(echo $segments); do
    echo "Processing segment: $segment"
    warc_files=$(grep "$segment" warc_paths)
    if [ -z "$warc_files" ]; then
      echo "Error: segment not found."
      break
    fi
    mkdir "segment=$segment"

    # Loop through the selected files and download them
    while read -r line; do
        echo "$line"
        file_name=$(echo "$line" | cut -d'/' -f6)

        if grep -q "$file_name" "../$processed_warc_file"; then
            echo "Skipping $file_name: it was already processed"
        else
            # Increment the counter
            ((counter++))
            echo "#$counter: $file_name"
            # Download the file
            curl -s $base_path/$line > "segment=$segment/$file_name"

            if [ $counter -eq $n_file_per_segment ]; then
                break
            fi
        fi
    done <<< "$warc_files"

rm warc_paths

done