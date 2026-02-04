# Japanese Pangenome Pipeline - Figure 2 Scripts

SLURM-based bioinformatics pipelines for the Japanese Pangenome Project.

## Pipeline Components

### 1. Flagger Pipeline (`flagger_script/`)
HiFi reads → minimap2 alignment → Flagger coverage analysis

- **Input**: Manifest TSV with sample IDs, haplotype FASTAs, and HiFi FASTQ paths
- **Output**: Per-haplotype coverage files (`.cov.gz`)

See [`flagger_script/slurm_hifi_flagger_pipeline_complete/README.md`](flagger_script/slurm_hifi_flagger_pipeline_complete/README.md) for details.

### 2. YAK QV Pipeline
Assembly quality assessment using k-mer analysis from CRAM/HiFi reads.

Key scripts:
- `run_one_cram2yak_cleaned.sh` - Main per-sample script
- `array_cram2yak_cleaned.sbatch` - SLURM array wrapper
- `Fig2_2b_arraycommand_cleaned_submit.sh` - Submission script

### 3. QUAST Pipeline
Assembly evaluation against reference genomes (CHM13, GRCh38).

Key scripts:
- `quast_CHM13_script.sh`
- `quast_GRCh38_script.sh`

### 4. Manifest Generation
Scripts to prepare sample manifests from workflow outputs.

- `prep_hap_manifest*.sh` - Generate haplotype manifests
- `fai_maker*.sh` - Create FASTA index files

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

## Directory Structure

```
scripts/
├── flagger_script/          # Flagger coverage pipeline
│   └── slurm_hifi_flagger_pipeline_complete/
├── *cram2yak*.sh/sbatch     # YAK QV scripts
├── *quast*.sh               # QUAST evaluation scripts
├── prep_hap_manifest*.sh    # Manifest generation
└── README.md
```

## Notes

- All paths in scripts use placeholders - update for your cluster
- Manifests with actual data paths are not committed (contain cluster-specific paths)
- Log directories are gitignored
