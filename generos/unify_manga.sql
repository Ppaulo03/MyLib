UPDATE midia
SET generos_unificados = ARRAY(
  SELECT DISTINCT unnest_super_genre
  FROM (
    SELECT
      CASE TRIM(g)
        -- AÇÃO E AVENTURA
        WHEN 'Action' THEN ARRAY['Ação']
        WHEN 'Adventure' THEN ARRAY['Aventura']
        WHEN 'Avant Garde' THEN ARRAY['Aventura', 'Drama'] -- Mapeado conforme seu "Vanguarda"
        
        -- FANTASIA E SCI-FI
        WHEN 'Fantasy' THEN ARRAY['Fantasia']
        WHEN 'Supernatural' THEN ARRAY['Fantasia', 'Terror / Suspense']
        WHEN 'Sci-Fi' THEN ARRAY['Ficção Científica']
        
        -- EMOÇÃO, DRAMA E COTIDIANO
        WHEN 'Drama' THEN ARRAY['Drama']
        WHEN 'Award Winning' THEN ARRAY['Drama'] -- Mapeado conforme seu "Premiado"
        WHEN 'Slice of Life' THEN ARRAY['Drama', 'Realidade / Educação']
        WHEN 'Gourmet' THEN ARRAY['Realidade / Educação']
        
        -- ROMANCE
        WHEN 'Romance' THEN ARRAY['Romance']
        WHEN 'Boys Love' THEN ARRAY['Romance', 'Drama']
        WHEN 'Girls Love' THEN ARRAY['Romance', 'Drama']
        
        -- COMÉDIA
        WHEN 'Comedy' THEN ARRAY['Comédia']
        
        -- TENSÃO E MISTÉRIO
        WHEN 'Horror' THEN ARRAY['Terror / Suspense']
        WHEN 'Suspense' THEN ARRAY['Terror / Suspense']
        WHEN 'Mystery' THEN ARRAY['Terror / Suspense', 'Estratégia / Raciocínio']
        
        -- ESPORTES
        WHEN 'Sports' THEN ARRAY['Esportes / Competitivo']
        
        -- ADULTO (+18)
        WHEN 'Hentai' THEN ARRAY['Adulto']
        WHEN 'Erotica' THEN ARRAY['Adulto']
        WHEN 'Ecchi' THEN ARRAY['Adulto', 'Comédia']
        
        ELSE ARRAY[]::text[]
      END as mapped_array
    FROM unnest(generos) as g
  ) sub_map,
  unnest(sub_map.mapped_array) as unnest_super_genre
)
WHERE categoria = 'manga';