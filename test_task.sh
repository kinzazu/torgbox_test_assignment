#!/bin/bash

 # Signal handlers for graceful shutdown
 cleanup() {
     echo "Cleaning up temporary files..."
     if [ -n "$temp_dir" ] && [ -d "$temp_dir" ]; then
         rm -rf "$temp_dir"
     fi
     exit 0
 }

 trap cleanup SIGINT SIGTERM

# Timeout for the entire script (40 minutes)
# Sites configuration
declarations_site="https://fsa.gov.ru/opendata/7736638268-rds/"
certificates_site="https://fsa.gov.ru/opendata/7736638268-rss/"
request_timeout=5

 main () {
 # Create directory structure
 mkdir -p certificate/original declaration/original

 # Function to extract date from filename
 extract_date() {
     local filename="$1"
     # Extract date from filename like data-20250630-structure-20250714.7z
     echo "$filename" | sed -E "s/.*data-([0-9]{8})-structure.*/\1/g"
 }

 # Function to process data section
 process_section() {
     local site_url="$1"
     local section_type="$2"

     echo "Processing $section_type section..."

     # Get download link
     link=$(curl --connect-timeout $request_timeout --silent --fail "$site_url" -L |
            grep -A 1 "Гиперссылка (URL) на набор" | sed -n "s/.*href=\"\([^\"]*\).*/\1/p")

     if [ -z "$link" ]; then
         echo "Error: Could not find download link for $section_type"
         return 1
     fi

     # Get filename from link
     filename=$(basename "$link")

     echo "filename= $filename"

     cur_folder=$(pwd)
     original_path="${cur_folder}/${section_type}/original/${filename}"

     echo "original_path = $original_path"
     # проверка наличия файла
     if [ -f "$original_path" ]; then
         remote_size=$(curl --silent --head "$link" | grep -i content-length | awk "{print \$2}" | tr -d "\r")
         local_size=$(stat -c%s "$original_path" 2>/dev/null || echo "0")

         if [ "$remote_size" = "$local_size" ]; then
             echo "File $filename already exists with same size, skipping..."
             return 0
         fi
     fi

     # Download archive
     echo "Downloading $filename..."
     if ! curl --silent --fail "$link" -o "$original_path"; then
         echo "Error: Failed to download $filename"
         return 1
     fi

     # Extract date from filename
     arch_date=$(extract_date "$filename")
     year=${arch_date:0:4}

     # Create year directory
     mkdir -p "${section_type}/${year}"

     # Create temporary directory for processing
     temp_dir=$(mktemp -d)
     cd "$temp_dir"

     # Extract archive
     echo 'if ! 7zz x "$original_path"; then'
     echo "if ! 7zz x "$original_path"; then"
     if ! 7zz x "$original_path" > /dev/null;  then
         echo "Error: Failed to extract $filename"
         rm -rf "$temp_dir"
         return 1
     fi

     # Process CSV files
     for csv_file in *.csv; do
         [ -f "$csv_file" ] || continue

         echo "Processing $csv_file..."

         # Read headers
         headers=$(head -n1 "$csv_file" | tr ";" "\n")
         header_count=$(echo "$headers" | wc -l)

         # Process data rows
         json_count=0
         zip_count=1

         # Create temporary directory for JSON files
         json_temp_dir=$(mktemp -d)

         # Skip header and process each row
         tail -n +2 "$csv_file" | while IFS=";" read -r line || [ -n "$line" ]; do
             json_count=$((json_count + 1))

             # Create JSON filename
             json_filename=$(printf "%s_%s_%07d.json" "$section_type" "$arch_date" "$json_count")
             json_path="${json_temp_dir}/${json_filename}"

             # Start JSON object
             echo "{" > "$json_path"

             # Add _type field
             echo "  \"_type\": \"$section_type\"," >> "$json_path"

             # Process each field
             field_num=1
             echo "$headers" | while read -r header; do
                 # Get field value
                 value=$(echo "$line" | cut -d";" -f"$field_num")

                 # Clean header and value
                 header=$(echo "$header" | sed "s/^\s*//;s/\s*$//")
                 value=$(echo "$value" | sed "s/^\s*//;s/\s*$//" | sed "s/\"/\\\\\"/g")

                 # Add field to JSON
                 if [ "$field_num" -eq "$header_count" ]; then
                     echo "  \"$header\": \"$value\"" >> "$json_path"
                 else
                     echo "  \"$header\": \"$value\"," >> "$json_path"
                 fi

                 field_num=$((field_num + 1))
             done

             # Close JSON object
             echo "}" >> "$json_path"

             # Create ZIP archive every 1000 files
             if [ $((json_count % 1000)) -eq 0 ]; then
                 zip_filename=$(printf "%s_%s_%03d.zip" "$section_type" "$arch_date" "$zip_count")
                 zip_path="../${section_type}/${year}/${zip_filename}"

                 cd "$json_temp_dir"
                 zip -q "$zip_path" *.json
                 rm *.json
                 cd "$temp_dir"

                 zip_count=$((zip_count + 1))
                 echo "Created archive: $zip_filename"
             fi
         done

         # Create final ZIP archive for remaining files
         remaining_files=$(ls "$json_temp_dir"/*.json 2>/dev/null | wc -l)
         if [ "$remaining_files" -gt 0 ]; then
             zip_filename=$(printf "%s_%s_%03d.zip" "$section_type" "$arch_date" "$zip_count")
             zip_path="../${section_type}/${year}/${zip_filename}"

             cd "$json_temp_dir"
             zip -q "$zip_path" *.json
             cd "$temp_dir"

             echo "Created final archive: $zip_filename"
         fi

         rm -rf "$json_temp_dir"
     done

     # Cleanup
     cd ..
     rm -rf "$temp_dir"

     echo "Completed processing $section_type"
 }

 # Main processing
 echo "Starting data processing..."

 # Process declarations
 if ! process_section "$declarations_site" "declaration"; then
     echo "Error processing declarations"
     exit 1
 fi

 # Process certificates
 if ! process_section "$certificates_site" "certificate"; then
     echo "Error processing certificates"
     exit 1
 fi

 echo "All processing completed successfully!"
}

main
