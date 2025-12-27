CREATE OR REPLACE FUNCTION public.match_filmes_inteligente(
  filmes_json jsonb,          -- Recebe a lista completa de objetos do Python
  match_minimo float4 DEFAULT 0.7
)
RETURNS TABLE (
    filme_completo jsonb
)
LANGUAGE plpgsql
STABLE
SET search_path = public, extensions, pg_catalog
AS $$
BEGIN
 
    PERFORM set_config('pg_trgm.word_similarity_threshold', '0.3', true);

    RETURN QUERY
    SELECT 
        item || jsonb_build_object(
            'supabase_id', best_match.id,
            'match_score', best_match.score,
            'match_title', best_match.titulo,
            'match_year', best_match.ano_lancamento
        ) AS filme_completo
    FROM jsonb_array_elements(filmes_json) AS item
    LEFT JOIN LATERAL (
        SELECT 
            m.id,
            m.titulo,
            m.ano_lancamento,
            calc.best_similarity AS score
        FROM public.midia m
        CROSS JOIN LATERAL (
            -- Calcula similaridade (Título Principal + Alternativos)
            SELECT MAX(
                CASE 
                    WHEN public.f_unaccent(t_cand) ILIKE public.f_unaccent((item->>'title')::text) THEN 1.0
                    ELSE similarity(
                        LOWER(TRIM(public.f_unaccent(t_cand))), 
                        LOWER(TRIM(public.f_unaccent((item->>'title')::text)))
                    )::float4
                END
            ) as best_similarity
            FROM unnest(array_append(COALESCE(m.titulos_alternativos, '{}'::text[]), m.titulo)) as t_cand
        ) calc
        WHERE 
            -- 1. Filtro de Ano (Usa índice B-Tree)
            m.ano_lancamento BETWEEN ((item->>'year')::int - 1) AND ((item->>'year')::int + 1)
            
            -- 2. Filtro de Texto Rápido (Usa índice GIN com operador %)
            AND (
                public.f_unaccent(m.titulo) % public.f_unaccent((item->>'title')::text)
                OR 
                (m.titulos_alternativos IS NOT NULL AND public.f_processar_alternativos(m.titulos_alternativos) %> public.f_unaccent((item->>'title')::text))
            )
            
        ORDER BY 
            calc.best_similarity DESC,
            m.rating DESC
        LIMIT 1
    ) best_match ON true;
END;
$$;