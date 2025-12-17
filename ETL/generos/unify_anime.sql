UPDATE midia
SET generos_unificados = ARRAY(
  SELECT DISTINCT unnest_super_genre
  FROM (
    SELECT
      CASE TRIM(g)
        -- AÇÃO E AVENTURA
        WHEN 'Ação' THEN ARRAY['Ação']
        WHEN 'Aventura' THEN ARRAY['Aventura']
        WHEN 'Vanguarda' THEN ARRAY['Aventura', 'Drama'] -- Experimental
        
        -- FANTASIA E SCI-FI
        WHEN 'Fantasia' THEN ARRAY['Fantasia']
        WHEN 'Sobrenatural' THEN ARRAY['Fantasia', 'Terror / Suspense']
        WHEN 'Ficção Científica' THEN ARRAY['Ficção Científica']
        
        -- EMOÇÃO, DRAMA E COTIDIANO
        WHEN 'Drama' THEN ARRAY['Drama']
        WHEN 'Premiado' THEN ARRAY['Drama']
        WHEN 'Slice of Life' THEN ARRAY['Drama', 'Realidade / Educação'] -- Cotidiano conecta com biografias
        WHEN 'Gourmet' THEN ARRAY['Realidade / Educação'] -- Foco em habilidade/culinária
        
        -- ROMANCE
        WHEN 'Romance' THEN ARRAY['Romance']
        WHEN 'Boys Love' THEN ARRAY['Romance', 'Drama']
        WHEN 'Girls Love' THEN ARRAY['Romance', 'Drama']
        
        -- COMÉDIA
        WHEN 'Comédia' THEN ARRAY['Comédia']
        
        -- TENSÃO E MISTÉRIO
        WHEN 'Terror' THEN ARRAY['Terror / Suspense']
        WHEN 'Suspense' THEN ARRAY['Terror / Suspense']
        WHEN 'Mistério' THEN ARRAY['Terror / Suspense', 'Estratégia / Raciocínio']
        
        -- ESPORTES
        WHEN 'Esportes' THEN ARRAY['Esportes / Competitivo']
        
        -- ADULTO (+18)
        WHEN 'Hentai' THEN ARRAY['Adulto']
        WHEN 'Erótico' THEN ARRAY['Adulto']
        WHEN 'Ecchi' THEN ARRAY['Adulto', 'Comédia'] -- Humor picante
        
        ELSE ARRAY[]::text[]
      END as mapped_array
    FROM unnest(generos) as g
  ) sub_map,
  unnest(sub_map.mapped_array) as unnest_super_genre
)
WHERE categoria = 'anime';