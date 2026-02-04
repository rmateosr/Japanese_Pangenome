# SLURM HiFi → minimap2 → Flagger (bam2cov per hap) pipeline (complete)

## What this pipeline produces (per sample ID)
Final products (under `FLAGGER_WORK_ROOT/<ID>/covfilesHap_direct/`):
- `<ID>.hap1.cov.gz` and `<ID>.hap1.cov.gz.index`
- `<ID>.hap2.cov.gz` and `<ID>.hap2.cov.gz.index`

Additional artifacts (shared output dirs):
- `minimap2_output/<ID>.mm2.sorted.bam` (+ `.bai`)
- `minimap2_output_samtools_flagstat/<ID>.mm2.sorted.flagstat.txt`
- `minimap2_output_cram/<ID>.mm2.sorted.cram` (+ `.crai`)  *(optional, see MAKE_CRAM)*

## Key behavior: temporary concatenated reference
The manifest contains **two assembly FASTAs** per sample: `fasta1_path` and `fasta2_path`.
For each SLURM array task, the script:
1. Concatenates hap1 + hap2 into a **temporary BGZF FASTA** on the compute node (`$SLURM_TMPDIR` if set, otherwise `/tmp`).
2. Adds `#1#` and `#2#` suffixes to contig names (if they are not already present) so hap contigs can be separated downstream.
3. Uses that temp reference for mapping and for generating hap BEDs.
4. Deletes the temp reference at the end of the job.

## Requirements (no `module load` assumed)
Commands must already be available on compute nodes:
- minimap2
- samtools
- bgzip (htslib)
- apptainer
- gzip, awk, zcat

## Manifest format
TSV header must include:
- `ID`
- `fasta1_path`
- `fasta2_path`
- `HiFi_paths` (one or more `.fastq.gz` paths; comma-separated if multiple)

## Running
1. Edit `OUT_ROOT` and `FLAGGER_SIF` in `00_submit_pipeline.sh`.
2. Submit:
   ```bash
   bash 00_submit_pipeline.sh /path/to/hap_manifestPacBio.tsv
   ```

## Notes / knobs
- `MAKE_CRAM=1` by default. You can disable CRAM creation:
  ```bash
  MAKE_CRAM=0 bash 00_submit_pipeline.sh manifest.tsv
  ```
- Many-FASTQ handling:
  - If a sample has many FASTQs, the script concatenates them into a temporary `*.fastq.gz` in node scratch for minimap2.
  - This threshold is `MAX_DIRECT_FASTQS` (default 40).
- Debug:
  - Set `DEBUG=1` to enable `set -x`.

## Expected equivalence to your original scripts
Your original workflow did:
- minimap2 mapping to a concatenated reference FASTA
- BAM sorting/indexing + flagstat
- BAM→CRAM
- split alignments into hap1/hap2 using the `#1#/#2#` contig naming convention
- run Flagger `bam2cov` per hap using a JSON mapping of hap1/hap2 BEDs

This SLURM pipeline preserves those semantics, but creates the concatenated reference *on the fly* per sample and deletes it after completion.
