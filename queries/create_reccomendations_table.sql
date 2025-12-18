CREATE OR REPLACE FUNCTION public.get_batch_recommendations(
  source_ids bigint[],
  source_types text[]
)
RETURNS TABLE (
  origem_id bigint,
  alvo_id bigint,
  alvo_categoria item_categoria,
  score float4
)
LANGUAGE plpgsql
SET search_path TO public, pg_catalog
AS $$
BEGIN
  RETURN QUERY
  SELECT
    r.origem_id,
    r.alvo_id,
    r.alvo_categoria,
    r.score
  FROM recommendations r
  JOIN (
    SELECT i1.val AS id, i2.val AS typ
    FROM unnest(source_ids) WITH ORDINALITY AS i1(val, ord)
    LEFT JOIN unnest(source_types) WITH ORDINALITY AS i2(val, ord) USING (ord)
  ) s2 ON r.origem_id = s2.id
      AND r.origem_categoria::text = s2.typ;
END;
$$;