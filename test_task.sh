#!/bin/bash

# signal handlers for graceful shutdown
cleanup() {
   echo "Cleaning up temporary files..."
   if [ -n "$temp_dir" ] && [ -d "$temp_dir" ]; then
       rm -rf "$temp_dir"
   fi
   if [ -n "$json_temp_dir" ] && [ -d "$json_temp_dir" ]; then
       rm -rf "$temp_dir"
   fi
   exit 0
}

 trap cleanup SIGINT SIGTERM


declarations_site="https://fsa.gov.ru/opendata/7736638268-rds/"
certificates_site="https://fsa.gov.ru/opendata/7736638268-rss/"
request_timeout=5

mkdir -p certificate/original declaration/original

# function to extract date from filename
extract_date() {
   local filename="$1"
   # Extract date from filename like data-20250630-structure-20250714.7z
   echo "$filename" | sed -E "s/.*data-([0-9]{8})-structure.*/\1/g"
}

# function to process data section
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

   working_dir=$(pwd)
   original_path="${working_dir}/${section_type}/original/${filename}"

   echo "original_path = $original_path"
   # check if file exists
   if [ -f "$original_path" ]; then
       remote_size=$(curl --silent --head "$link" | grep -i content-length | awk "{print \$2}" | tr -d "\r")
       local_size=$(stat -c%s "$original_path" 2>/dev/null || echo "0")

       if [ "$remote_size" = "$local_size" ]; then
           echo "File $filename already exists with same size, skipping..."
           return 0
       fi
   fi

#  download archive
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
   cd "$temp_dir" || exit

   # Extract archive
   if ! 7zz x "$original_path" > /dev/null;  then
       echo "Error: Failed to extract $filename"
       rm -rf "$temp_dir"
       return 1
   fi

   # Process CSV files
   for csv_file in *.csv; do
       [ -f "$csv_file" ] || continue

       echo "Processing $csv_file..."

       json_temp_dir=$(mktemp -d)
#         cd "$json_temp_dir"


  # Use AWK for much faster processing
  gawk -F';' -v section_type="$section_type" -v arch_date="$arch_date" -v year="$year" -v temp_dir="$json_temp_dir" \
      -v working_dir="$working_dir" '
        BEGIN {
            json_count = 0
            zip_count = 1
            files_in_current_zip = 0
        }

        NR==1 {
            # Store headers without quotes
            for(i=1; i<=NF; i++) {
                # Remove leading/trailing whitespace and quotes
                gsub(/^[ \t\r"]*|[ \t\r"]*$/, "", $i)
                headers[i] = $i
            }
            header_count = NF
            next
        }

        {
            json_count++
            files_in_current_zip++

            json_filename = sprintf("%s_%s_%07d.json", section_type, arch_date, json_count)
            json_path = temp_dir "/" json_filename

            # Start JSON
            printf "{\n" > json_path
            printf "  \"_type\": \"%s\"", section_type > json_path

            for(i=1; i<=NF && i<=header_count; i++) {
                field_value = $i

                # Clean field value - remove outer quotes and whitespace
                gsub(/^[ \t\r"]*|[ \t\r"]*$/, "", field_value)

                # Escape internal quotes and other JSON special characters
                gsub(/\\/, "\\\\", field_value)
                gsub(/"/, "\\\"", field_value)
                gsub(/\n/, "\\n", field_value)
                gsub(/\r/, "\\r", field_value)
                gsub(/\t/, "\\t", field_value)

                printf ",\n  \"%s\": \"%s\"", headers[i], field_value > json_path
            }

            printf "\n}\n" > json_path
            close(json_path)

            # Create ZIP every 1000 files
            if(files_in_current_zip == 1000) {
                zip_filename = sprintf("%s_%s_%03d.zip", section_type, arch_date, zip_count)
                zip_path = working_dir "/" section_type "/" year "/" zip_filename

                system("cd " temp_dir " && zip -q " zip_path " *.json && rm *.json")
                zip_count++
                files_in_current_zip = 0
                print "Created archive: " zip_filename
            }
        }

        END {
            # Handle remaining files
            if(files_in_current_zip > 0) {
                zip_filename = sprintf("%s_%s_%03d.zip", section_type, arch_date, zip_count)
                zip_path = working_dir "/" section_type "/" year "/" zip_filename

                system("cd " temp_dir " && zip -q " zip_path " *.json")
                print "Created final archive: " zip_filename
            }

            print "Processed " json_count " records"
        }

  ' "${csv_file}"

   done

   # Cleanup
   cd "$working_dir" || exit
   rm -rf "$temp_dir"

   echo "Completed processing $section_type"
}

# Main processing
echo "Starting data processing..."

# Process certificates
if ! process_section "$certificates_site" "certificate"; then
   echo "Error processing certificates"
   exit 1
fi

# Process declarations
if ! process_section "$declarations_site" "declaration"; then
   echo "Error processing declarations"
   exit 1
fi

echo "All processing completed successfully!"
