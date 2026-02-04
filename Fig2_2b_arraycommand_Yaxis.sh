MANIFEST="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest.tsv"
n=$(( $(wc -l < "$MANIFEST") - 1 ))

#sbatch --array=1-"$n" \
sbatch --array=1-"$n"%10 \
  --export=ALL \
  /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/concat_mm2_flagstat_hifi_array.sbatch
