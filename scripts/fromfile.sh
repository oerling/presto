grep '"' $1 /tmp/ts
sed --in-place 's/\\n\" +//' /tmp/ts
sed --in-place 's/[ ]*\"//g' /tmp/ts

