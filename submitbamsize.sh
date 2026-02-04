MANIFEST="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifest.tsv"
n=$(( $(wc -l < "$MANIFEST") - 1 ))

#sbatch --array=1-"$n" \
#sbatch --array=1-"$n"%10 \
#--job-name=bamsize_hif \
#  --export=ALL \
#  /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/size_concat_mm2_bamsize_array.sbatch



sbatch --array=1-2 \
  --job-name=bamsize_hif \
  --export=ALL \
  /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/size_concat_mm2_bamsize_array_fewsamplestest.sbatch
