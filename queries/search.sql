CREATE OR REPLACE FUNCTION public.buscar_midias(
  termo_busca text,
  filtro_ano int DEFAULT NULL,
  filtro_categoria text DEFAULT NULL,
  match_minimo float4 DEFAULT 0.8
)
RETURNS TABLE (
    id bigint,
    categoria text,
    titulo text,
    descricao text,
    ano_lancamento int,
    imagem text,
    metadata jsonb,
    generos text[],    
    generos_unificados text[],
    rating numeric,
    classificacao numeric,
    score_similaridade float4
)
LANGUAGE plpgsql
STABLE
SET search_path = public, extensions, pg_catalog
AS $$
BEGIN
    PERFORM set_config('pg_trgm.word_similarity_threshold', '0.2', true);

    RETURN QUERY
    SELECT 
        m.id, 
        m.categoria, 
        m.titulo, 
        m.descricao, 
        m.ano_lancamento, 
        m.imagem, 
        m.metadata, 
        m.generos, 
        m.generos_unificados, 
        m.rating, 
        m.classificacao::numeric,
        calc.best_similarity AS score_similaridade

    FROM public.midia m
    
    CROSS JOIN LATERAL (
        SELECT 
            CASE 
                WHEN termo_busca IS NULL OR termo_busca = '' THEN 1.0
                ELSE MAX(
                    CASE 
                        WHEN public.f_unaccent(t_cand) ILIKE ('%' || public.f_unaccent(termo_busca) || '%') 
                        THEN 1.0
                        ELSE similarity(
                            LOWER(TRIM(public.f_unaccent(t_cand))), 
                            LOWER(TRIM(public.f_unaccent(termo_busca)))
                        )::float4
                    END
                )
            END as best_similarity,
            
            CASE 
                WHEN termo_busca IS NULL OR termo_busca = '' THEN 0.0
                ELSE MIN(
                    LOWER(TRIM(public.f_unaccent(t_cand))) <-> LOWER(TRIM(public.f_unaccent(termo_busca)))
                )
            END as best_distance
            
        FROM unnest(array_append(COALESCE(m.titulos_alternativos, '{}'::text[]), m.titulo)) as t_cand
    ) calc

    WHERE 
        (
            termo_busca IS NULL OR termo_busca = '' 
            OR 
            (
                (
                  public.f_unaccent(m.titulo) % public.f_unaccent(termo_busca)
                  OR
                  public.f_unaccent(m.titulo) ILIKE ('%' || public.f_unaccent(termo_busca) || '%')
                )
                OR
                (
                  public.f_processar_alternativos(m.titulos_alternativos) %> public.f_unaccent(termo_busca)
                  OR
                  public.f_processar_alternativos(m.titulos_alternativos) ILIKE ('%' || public.f_unaccent(termo_busca) || '%')
                )
            )
        )
        AND (termo_busca IS NULL OR termo_busca = '' OR calc.best_similarity >= match_minimo)
        AND (filtro_ano IS NULL OR m.ano_lancamento = filtro_ano)
        AND (filtro_categoria IS NULL OR m.categoria = filtro_categoria)
    
    ORDER BY 
        CASE 
            WHEN termo_busca IS NOT NULL AND termo_busca <> '' THEN
                calc.best_distance - (COALESCE(m.rating, 0) / 20.0)
            ELSE 
                -COALESCE(m.rating, 0)
    END ASC
    LIMIT 50;
END;
$$;