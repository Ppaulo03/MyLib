CREATE OR REPLACE FUNCTION public.get_recommendations(
    p_consumed_ids bigint[],
    p_top_genres jsonb,
    p_limit int DEFAULT 10
)
RETURNS SETOF public.midia
LANGUAGE plpgsql
SET search_path TO public, pg_catalog
AS $$
DECLARE
    v_genre_keys text[];
    v_penalty numeric := 5.0; 
BEGIN
    
    SELECT ARRAY(SELECT jsonb_object_keys(p_top_genres)) INTO v_genre_keys;
    IF v_genre_keys IS NULL OR array_length(v_genre_keys, 1) IS NULL THEN
        RETURN QUERY
        SELECT m.*
        FROM (VALUES ('filme'::text), ('livro'::text), ('jogo'::text), ('anime'::text), ('serie'::text), ('manga'::text)) AS t(categoria_alvo)
        CROSS JOIN LATERAL (
            SELECT midia.* FROM public.midia 
            WHERE midia.categoria = t.categoria_alvo
            AND (p_consumed_ids IS NULL OR midia.id <> ALL(p_consumed_ids))
            ORDER BY midia.rating DESC 
            LIMIT p_limit
        ) m;
        RETURN;
    END IF;

    RETURN QUERY
    WITH genre_weights AS (
        SELECT key AS genero, value::numeric AS weight 
        FROM jsonb_each_text(p_top_genres)
    )
    SELECT (candidates_pool.m).*
    FROM (VALUES ('filme'::text), ('livro'::text), ('jogo'::text), ('anime'::text), ('serie'::text), ('manga'::text)) AS t(categoria_alvo)
    CROSS JOIN LATERAL (
        SELECT 
            sub.m
        FROM (
            SELECT 
                m_inner AS m,
                (
                   (
                       SELECT SUM(COALESCE(gw.weight, -v_penalty))
                       FROM unnest(m_inner.generos_unificados) g
                       LEFT JOIN genre_weights gw ON gw.genero = g
                   ) * 10
                ) + COALESCE(m_inner.rating, 0) AS final_score
            FROM public.midia m_inner
            WHERE 
                m_inner.categoria = t.categoria_alvo
                AND m_inner.generos_unificados && v_genre_keys
                AND COALESCE(m_inner.rating, 0) >= 2.0
                AND (p_consumed_ids IS NULL OR m_inner.id <> ALL(p_consumed_ids))
        ) AS sub
        ORDER BY sub.final_score DESC, (sub.m).rating DESC
        LIMIT p_limit
    ) AS candidates_pool;
END;
$$;