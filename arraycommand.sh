MANIFEST=/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Assemblies/hap_manifest.tsv
N=$(( $(wc -l < "$MANIFEST") - 1 ))
sbatch --array=1-"$N"%50 copy_haps_array.sh