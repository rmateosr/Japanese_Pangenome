#MANIFEST="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest.tsv"
#n=$(($(wc -l < "$MANIFEST") - 1))
#
#
#sbatch --array=1-"$n" \
#  --export=ALL,MANIFEST="$MANIFEST" \
#  /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/array_cram2yak_100kremoval.sbatch


MANIFEST="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest.tsv"

sbatch --array=1-5 \
  --export=ALL,MANIFEST="$MANIFEST" \
  /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/array_cram2yak_100kremoval_andl100k.sbatch
