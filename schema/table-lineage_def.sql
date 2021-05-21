CREATE TABLE IF NOT EXISTS lineage_def (
	variant_id             text,
	pango                  text,
	nextstrain             text,
	ref_pos_alt            text,
	codon_change           text,
	gene                   text,
	pos                    int,
	predicted_effect       text,
	protein                text,
	protein_codon_position int,
	ref                    text,
	type                   text,
	alt                    text,
	amino_acid_change      text,
	description            text,
	snp_codon_position   text
);

