# Japanese Pangenome Pipeline - Session Notes
#bioinformatics #flagger #pangenome #slurm

> **Date:** 2026-02-04
> **Purpose:** Set up and test Flagger HiFi coverage pipeline

---

## Summary

Set up a SLURM-based pipeline for running Flagger coverage analysis on 47 Japanese Pangenome samples. The pipeline maps HiFi reads to concatenated haplotype assemblies and generates per-haplotype coverage files.

---

## What We Did

### 1. Reviewed Pipeline Scripts
- Location: `flagger_pipeline/slurm_hifi_flagger_pipeline_complete/`
- Scripts validated for syntax and logic
- Key files:
  - `00_submit_pipeline.sh` - SLURM submission wrapper
  - `01_per_sample_pipeline.sbatch` - Per-sample processing

### 2. Organized Repository Structure
Created organized directory structure and pushed to GitHub:

```
scripts/
├── Fig2b_yak_qv/              # YAK QV analysis
├── assembly_evaluation/        # QUAST scripts
├── flagger_pipeline/           # Flagger coverage (main focus)
├── manifest_generation/        # Manifest prep
├── mapping_bam_processing/     # BAM/CRAM utilities
└── utilities/                  # Helper scripts
```

**GitHub repo:** https://github.com/rmateosr/Japanese_Pangenome

### 3. Downloaded Flagger Container
```bash
apptainer pull docker://mobinasri/flagger:v1.1.0
```
- Location: `/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/containers/flagger_v1.1.0.sif`
- Size: 1.9 GB

### 4. Configured Pipeline Paths
Updated `00_submit_pipeline.sh`:
| Variable | Value |
|----------|-------|
| `OUT_ROOT` | `/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/flagger_output` |
| `FLAGGER_SIF` | `/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/containers/flagger_v1.1.0.sif` |

### 5. Validated Manifest
- Manifest: `/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifestPacBio.tsv`
- 47 samples (NA18939 - NA19091)
- Columns: `ID`, `fasta1_path`, `fasta2_path`, `HiFi_paths`

### 6. Ran Direct Test
- Created single-sample test manifest (`test_manifest_1sample.tsv`)
- Ran script directly with `DEBUG=1`
- **Result:** All validations passed (tools, files, manifest parsing)

### 7. Submitted Test Job
```bash
bash 00_submit_pipeline.sh test_manifest_1sample.tsv
```
- **Job ID:** 14798218
- **Sample:** NA18939
- **Status:** Pending/Running

---

## Pipeline Workflow

```
Input: Manifest TSV
  ↓
1. Concatenate hap1 + hap2 FASTAs (temp reference with #1#/#2# markers)
  ↓
2. minimap2 map-hifi → sorted BAM
  ↓
3. samtools flagstat
  ↓
4. BAM → CRAM (optional)
  ↓
5. Split BAM by haplotype (using BED regions)
  ↓
6. Flagger bam2cov per haplotype
  ↓
Output: *.hap1.cov.gz, *.hap2.cov.gz
```

---

## Key Paths

| Item | Path |
|------|------|
| Scripts | `/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/scripts/` |
| Flagger pipeline | `scripts/flagger_pipeline/slurm_hifi_flagger_pipeline_complete/` |
| Container | `/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/containers/flagger_v1.1.0.sif` |
| Output | `/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/flagger_output/` |
| Manifest | `/lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifestPacBio.tsv` |
| Input data | `/lustre9/open/shared_data/visc/` |

---

## Next Steps

- [ ] Wait for test job (14798218) to complete
- [ ] Verify outputs exist:
  - `flagger_output/flagger_work/NA18939/covfilesHap_direct/NA18939.hap1.cov.gz`
  - `flagger_output/flagger_work/NA18939/covfilesHap_direct/NA18939.hap2.cov.gz`
- [ ] If successful, submit all 47 samples:
  ```bash
  bash 00_submit_pipeline.sh /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifestPacBio.tsv
  ```
- [ ] Commit updated scripts to GitHub

---

## Useful Commands

```bash
# Check job status
squeue -j 14798218

# Watch logs
tail -f log/hifi_mm2_flagger.14798218.1.out

# Check outputs
ls -lh flagger_output/flagger_work/NA18939/covfilesHap_direct/

# Submit all samples (after test succeeds)
bash 00_submit_pipeline.sh /lustre10/home/raulnmateos/Japanese_Pangenome/Pipeline/Figure_2/required_data/Manifest/hap_manifestPacBio.tsv
```

---

## Issues Encountered

1. **GitHub push auth:** GitHub no longer accepts passwords; needed Personal Access Token
2. **Repo conflict:** Had to create new empty repo (Japanese_Pangenome) instead of using existing one with content
3. **SLURM queue:** Jobs pending due to Priority; used direct test to validate script logic

---

## Related Files
- [[hap_manifestPacBio.tsv]] - Sample manifest
- [[00_submit_pipeline.sh]] - Submission script
- [[01_per_sample_pipeline.sbatch]] - Main pipeline script
