-- 'convert_single_aa'
-- This function converts the single letter amino acid symbols to three letter amino acid symbol
-- Sample:
-- convert_single_aa('A63T')
-- 'p.Ala63Thr'

CREATE FUNCTION convert_single_aa(char(10)) RETURNS char(10) AS $$
    WITH aa_left_three AS (
      SELECT aa_three_letter_symbol 
      FROM amino_acid_symbol 
      WHERE aa_single_letter_symbol=(SELECT LEFT($1, 1) AS ExtractString)
      )
   , aa_right_three AS (
      SELECT aa_three_letter_symbol 
      FROM amino_acid_symbol 
      WHERE aa_single_letter_symbol=(SELECT RIGHT($1, 1) AS ExtractString)
      )
   , left_replaced AS (
      SELECT REPLACE($1, 
              (SELECT LEFT($1, 1) AS ExtractString),
              (SELECT CONCAT('p.', 
                            (SELECT * FROM aa_left_three)))))
   SELECT REPLACE((SELECT * FROM left_replaced), 
              (SELECT RIGHT($1, 1) AS ExtractString),
              (SELECT * FROM aa_right_three));
$$ LANGUAGE SQL;

----------------------------------------------------------------------------
