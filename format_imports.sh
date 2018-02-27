startImports=false
stopImports=false
while IFS='' read -r line || [[ -n "$line" ]]; do
  if [[ $line == *"import"* ]]
  then
    startImports=true
  fi
  if [[ $line == ")" ]]
  then
    stopImports=true
  fi
  if $startImports && $stopImports
  then
      echo "Text read from file: $line"
  fi
done < "tsdb/remote/server.go"
