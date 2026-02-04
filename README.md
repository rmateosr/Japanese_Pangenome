# Japanese Pangenome Pipeline

SLURM-based bioinformatics pipelines for the Japanese Pangenome Project.

## Directory Structure

```
├── Fig2b_yak_qv/               # YAK k-mer QV analysis (Figure 2b)
├── assembly_evaluation/         # QUAST assembly evaluation
├── flagger_pipeline/            # Flagger HiFi coverage analysis
│   ├── slurm_hifi_flagger_pipeline/
│   └── slurm_hifi_flagger_pipeline_complete/
├── manifest_generation/         # Sample manifest & FAI index preparation
├── mapping_bam_processing/      # minimap2 mapping, BAM/CRAM utilities
├── utilities/                   # Miscellaneous helper scripts
└── README.md
```

## Pipeline Components

### 1. Figure 2b - YAK QV Analysis (`Fig2b_yak_qv/`)
Assembly quality assessment using k-mer analysis from CRAM/HiFi reads.

Key scripts:
- `run_one_cram2yak_cleaned.sh` - Main per-sample QV script
- `array_cram2yak_cleaned.sbatch` - SLURM array wrapper
- `Fig2_2b_arraycommand_cleaned_submit.sh` - Submission script

### 2. Assembly Evaluation (`assembly_evaluation/`)
QUAST-based assembly evaluation against reference genomes.

- `quast_CHM13_script.sh` - Evaluate against CHM13
- `quast_GRCh38_script.sh` - Evaluate against GRCh38

### 3. Flagger Pipeline (`flagger_pipeline/`)
HiFi reads → minimap2 alignment → Flagger coverage analysis.

- **Input**: Manifest TSV with sample IDs, haplotype FASTAs, HiFi FASTQ paths
- **Output**: Per-haplotype coverage files (`.cov.gz`)

See [`flagger_pipeline/slurm_hifi_flagger_pipeline_complete/README.md`](flagger_pipeline/slurm_hifi_flagger_pipeline_complete/README.md) for details.

### 4. Manifest Generation (`manifest_generation/`)
Scripts to prepare sample manifests from workflow outputs.

- `prep_hap_manifest*.sh` - Generate haplotype manifests
- `fai_maker*.sh` - Create FASTA index files
- `Step0_manifest_generation*.sh` - Initial manifest setup

### 5. Mapping & BAM Processing (`mapping_bam_processing/`)
Read alignment and BAM/CRAM processing utilities.

- `map_hifi_flagstat.sh` - HiFi mapping with flagstat
- `concat_hifi_per_sample.sh` - Concatenate HiFi reads
- `run_one_mito_removal.sh` - Mitochondrial read filtering

### 6. Utilities (`utilities/`)
Helper scripts for file management and testing.

- `copy_haps_array.sh` - Copy haplotype files
- `fix_permissions_by_type.sh` - Fix file permissions

## Requirements

- SLURM cluster
- minimap2, samtools, bgzip (htslib)
- apptainer (for Flagger container)
- yak (for QV analysis)
- QUAST (for assembly evaluation)

## Usage

1. Prepare your manifest TSV (see examples in each pipeline directory)
2. Edit paths in submission scripts (`OUT_ROOT`, container paths, etc.)
3. Submit via the appropriate `*_submit.sh` or `*_arraycommand*.sh` script

## Notes

- All paths in scripts use placeholders - update for your cluster
- Manifests with actual data paths are not committed (contain cluster-specific paths)
- Log directories are gitignored
