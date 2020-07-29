################################################################################
#
# Replace/remove unacceptable Unicodes or formats by RegEx.
# This script is used for DBPedia and Wikidata.
# All unacceptable unicodes or formats were also came from the two datasets.
# It only supports NTriple RDF formats.
#
# Run:
#
#   $ ./refineNT.sh [DIRECTORY HAVING NT RDFs]
#
################################################################################

#!/bin/bash

EXTEND=".nt"

# Unacceptable unicodes for URL.
T_UNICODES=( 'FFFA' 'FFFB' 'FFFC' 'A711' 'A712'
             'FFFD' 'FFFE' 'FFFF' 'A710' 'A713'
             'FFF0' 'FFF1' 'FFF2' 'A70F' 'A714'
             'FFF3' 'FFF4' 'FFF5' 'A70E' 'A715'
             'FFF6' 'FFF7' 'FFF8' 'A70D' 'A716'
             'FFF9' '0027' '0025' 'A70C' 'A720'
             '0060' '00B4' '2018' 'A70B' 'A721'
             '2019' '201C' '201D' 'A70A' 'A789'
             '0023' '0001' 'A709' 'A78A' '00BB'
             '0002' '0003' '0004' 'A708' 'AB5B'
             '0005' '0006' '0007' 'A707' 'AB6A'
             '0008' 'A704' 'A705' 'A706' 'AB6B'
             '000E' '000F' '0010' 'A703' 'FBB2'
             '0011' '0012' '0013' 'A702' 'FBB3'
             '0014' '0015' '0016' 'A701' 'FBB4'
             '0017' '0018' '0019' 'A700' 'FBB5'
             '001A' '001B' '001C' '309C' 'FBB6'
             '001D' '001E' '001F' '309B' 'FBB7'
             '0080' '0081' '1FFD' '1FFE' 'FBB8'
             '0082' '0083' '0084' '1FEE' 'FBB9'
             '0085' '0086' '0087' '1FED' 'FBBA' 
             '0088' '0089' '008A' '1FDF' 'FBBB'
             '008B' '008C' '008D' '1FDD' 'FBBC'
             '008E' '008F' '0090' '1FCF' 'FBBD'
             '0091' '0092' '0093' '1FCE' 'FBBE'
             '0094' '0095' '0096' '1FCD' 'FBBF'
             '0097' '0098' '0099' '1FC1' 'FBC0'
             '009A' '009B' '009C' '1FC0' 'FBC1'
             '009D' '009E' '009F' '1FBF' 'FF3E'
             '005E' '00A8' '00AF' '1FBD' 'FF40'
             '00B8' '02C4' '02C5' '0385' 'FFE3'
             '02D2' '02D3' '02D4' '0384' '1F3FB'
             '02D5' '02D6' '02D8' '0375' '1F3FC'
             '02D9' '02DA' '02DB' '02FF' '1F3FD'
             '02DC' '02DD' '02DE' '02FE' '1F3FE'
             '02DF' '02E5' '02E6' '02FD' '1F3FF'
             '02E7' '02E8' '02E9' '02FC' '203A'
             '02EA' '02EB' '02ED' '02FB' '2E03'
             '02EF' '02F0' '02F1' '02FA' '2E05'
             '02F2' '02F3' '02F4' '02F9' '2E0A'
             '2E0D' '2E1D' '2E21' '0022'
             '02F5' '02F6' '02F7' '02F8'
          )

DIR=$1

SED_PARAM=""

# Remove unacceptable unicodes from URIs. 
# e.g. <^"'..> --> <..>
for ucode in ${T_UNICODES[@]}
do
  echo $ucode
  SED_PARAM+="s/\(.*<[^>]*\)\\\u${ucode}\([^<]*>.*\)/\1r\2/g;"
  SED_PARAM+="s/\(.*<[^>]*\)$(echo -ne '\u'${ucode})\([^<]*>.*\)/\1r\2/g;"
done
# Remove space from URIs.
# e.g. <https://test. . . .> --> <http://test....>
SED_PARAM+="s/\(.*<[^>]*\)\s\([^<]*>.*\)/\1r\2/g;"
echo "Ucode construction done .."

SED_PARAM=":begin;"${SED_PARAM}

# remove URL starts from _, but not followed by :.
SED_PARAM=${SED_PARAM}"/^_[^:]/d;"
# Replace IPv4 URIs to replaced URIs.
# It is very rough filtering since it replaces valid IPv4 URIs too.
SED_PARAM+="s/<http:\\/\\/[0-9]+[^>]*>/<http:\\/\\/replaced.com>/g;"
SED_PARAM+="s/<https:\\/\\/[0-9]+[^>]*>/<https:\\/\\/replaced.com>/g;"
SED_PARAM+="s/<ftp:\\/\\/[0-9]+[^>]*>/<ftp:\\/\\/replaced.com>/g;"
SED_PARAM+="t begin"

echo "Target DIR:" $1

for f in `find ${DIR} -name "*${EXTEND}"`;
do
  fname=${f##*/}
  echo $fname
  if [[ $fname == *'extract'* ]]; then
    continue
  fi
  echo "Processing $fname file.."
  sed -e "${SED_PARAM}" $DIR$fname > ${DIR}/extract_${fname}
  echo "sed -e '${SED_PARAM}' $DIR$fname > ${DIR}/extract_${fname}"
done
