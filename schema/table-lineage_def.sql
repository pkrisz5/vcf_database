CREATE TABLE IF NOT EXISTS lineage_def (
	variant_id             text,
	pango                  text,
	type_variant           text,
	amino_acid_change      text,
	protein_codon_position int,
	ref_protein            text,
	alt_protein            text,
	gene                   text,
	effect                 text,
	snpeff_original_mut    text,
	ref_pos_alt            text,
	ref                    text,
	alt                    text,
	pos                    int,
	description            text

);

