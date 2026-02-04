#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------
# Rationale for changes:
#  - Create log directory BEFORE submission (Slurm opens logs at job start).
#  - Submit sequentially with dependency to avoid both jobs landing on the same node
#    and fighting for I/O/CPU (very common cause of Ågno updates for a dayÅh).
# ------------------------------------------------------------

FIG2="/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2"
SCRIPTS="${FIG2}/scripts"

mkdir -p "${FIG2}/log"

jid_gr=$(sbatch --parsable "${SCRIPTS}/quast_GRCh38_script.sh")
echo "Submitted GRCh38 QUAST as job ${jid_gr}"

jid_ch=$(sbatch --parsable "${SCRIPTS}/quast_CHM13_script.sh")
echo "Submitted CHM13 QUAST as job ${jid_ch}"
