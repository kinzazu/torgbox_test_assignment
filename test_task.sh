#!/bin/bash


#certificates=
#declarations=
#
#
#maketemp() {
#    
#}
#
#
#unarch_file () {
#    7z x $1
#
#}
#
#download_archive() {
#curl $1 -L -O
#
#
#
#}
#
#
#parse_url() {
#   curl $1 -L | grep ""
#}
#
#
#

# download declarations
# 1 find new lind

main_folder=$(pwd)
echo $main_folder

tmp_dir=$(mktemp -d)
echo $tmp_dir

link_to_declaration=$(curl --silent https://fsa.gov.ru/opendata/7736638268-rds -L | grep "Гиперссылка (URL) на набор" -A 1 |  sed -n 's/.*href="\([^"]*\).*/\1/p')


cd $tmp_dir
filename=(curl $link_to_declaration -O --silent -w "%{filename_effective}")
echo $filename

7z x *.7z

headers=(cat rds_01062025-30062025.csv | head -2 | tr ";" " ")
