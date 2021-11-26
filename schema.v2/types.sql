-- common types

CREATE TYPE ebi.type_integrity AS ENUM ('ok', 'empty file', 'corrupt file');

-- vcf table related types

CREATE TYPE ebi.type_lof AS ENUM (
    '(S|GU280_gp02|1|1.00)', '(ORF7a|GU280_gp07|1|1.00)', 
    '(E|GU280_gp04|1|1.00)', '(ORF7b|GU280_gp08|1|1.00)', 
    '(ORF10|GU280_gp11|1|1.00)', '(ORF1ab|GU280_gp01|1|1.00)', 
    '(M|GU280_gp05|1|1.00)', '(N|GU280_gp10|1|1.00)', 
    '(ORF8|GU280_gp09|1|1.00)', '(ORF3a|GU280_gp03|1|1.00)', 
    '(ORF6|GU280_gp06|1|1.00)'
);

CREATE TYPE ebi.type_nmd AS ENUM ('(ORF1ab|GU280_gp01|1|1.00)');

CREATE TYPE ebi.type_genename AS ENUM (
    'ORF1ab-S', 'S', 'ORF7a', 'ORF6-ORF7a', 'M-ORF6', 
    'ORF8-N', 'E-M', 'ORF7b&ORF8', 'ORF1ab&S', 'N', 
    'ORF10-CHR_END', 'ORF1ab', 'N-ORF10', 'ORF7a&ORF7b', 
    'ORF7b', 'ORF6', 'ORF3a', 'ORF10', 'ORF3a&S', 
    'CHR_START-ORF1ab', 'S-ORF3a', 'M&ORF6', 'ORF7b-ORF8', 
    'ORF3a-E', 'M', 'ORF6&ORF7a', 'E', 'M&ORF7a', 'ORF8',
    'E&ORF3a', 'ORF6&ORF7b', 'N&ORF10', 'E&M'
);

CREATE TYPE ebi.type_featuretype AS ENUM ('intergenic_region', 'transcript', 'gene_variant');

CREATE TYPE ebi.type_featureid AS ENUM (
    'GU280_gp02-GU280_gp03', 'GU280_gp09', 'GU280_gp07', 
    'GU280_gp06-GU280_gp07', 'GU280_gp05-GU280_gp06', 
    'GU280_gp05', 'GU280_gp01', 'GU280_gp10', 
    'GU280_gp08-GU280_gp09', 'GU280_gp08', 'GU280_gp01-GU280_gp02', 
    'GU280_gp03-GU280_gp04', 'GU280_gp04', 'GU280_gp10-GU280_gp11', 
    'GU280_gp11-CHR_END', 'CHR_START-GU280_gp01', 'GU280_gp11', 
    'GU280_gp09-GU280_gp10', 'GU280_gp03', 'GU280_gp02', 
    'GU280_gp06', 'GU280_gp04-GU280_gp05'
);

CREATE TYPE ebi.type_transcriptbiotype AS ENUM ('protein_coding');

CREATE TYPE ebi.type_rank AS ENUM ('1/1', '2/2', '1/2');

-- [VCF document](https://pcingola.github.io/SnpEff/se_inputoutput/#effect-prediction-details)
-- almost useless, different tokens

CREATE TYPE ebi.type_annotation AS ENUM (
    'frameshift_variant', 'missense_variant', 'disruptive_inframe_deletion', 
    'synonymous_variant', 'stop_gained', 'intergenic_region', 'conservative_inframe_deletion', 
    'stop_gained&disruptive_inframe_deletion', 'frameshift_variant&stop_gained', 
    'conservative_inframe_insertion', 'disruptive_inframe_insertion', 'start_lost',
    'splice_region_variant&stop_retained_variant', 'stop_lost&splice_region_variant', 
    'gene_fusion', 'initiator_codon_variant', 'stop_lost&conservative_inframe_deletion', 
    'frameshift_variant&start_lost', 'conservative_inframe_insertion&splice_region_variant',
    'stop_lost&disruptive_inframe_deletion', 'transcript_ablation',
    'start_lost&disruptive_inframe_insertion', 'start_lost&conservative_inframe_deletion',
    'missense_variant&splice_region_variant', 'stop_lost&disruptive_inframe_deletion&splice_region_variant',
    'frameshift_variant&stop_lost&splice_region_variant', 'start_lost&disruptive_inframe_deletion',
    'frameshift_variant&splice_region_variant', 'stop_gained&conservative_inframe_insertion',
    'stop_gained&disruptive_inframe_insertion', 'stop_lost', 'splice_region_variant&synonymous_variant',
    'stop_lost&conservative_inframe_deletion&splice_region_variant', 
    'disruptive_inframe_deletion&splice_region_variant', 'disruptive_inframe_insertion&splice_region_variant',
    'frameshift_variant&stop_lost', 'intragenic_variant', 'frameshift_variant&stop_gained&splice_region_variant',
    'stop_lost&disruptive_inframe_insertion&splice_region_variant', 'start_lost&conservative_inframe_insertion'
);

CREATE TYPE ebi.type_annotationimpact AS ENUM ('HIGH', 'MODERATE', 'LOW', 'MODIFIER');

