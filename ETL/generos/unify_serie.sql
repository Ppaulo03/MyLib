UPDATE midia
SET generos_unificados = ARRAY(
  SELECT DISTINCT unnest_super_genre
  FROM (
    SELECT
      CASE TRIM(g)
        -- AÇÃO E AVENTURA (Mapeando Action & Adventure e Faroeste)
        WHEN 'Action & Adventure' THEN ARRAY['Ação', 'Aventura']
        WHEN 'Faroeste' THEN ARRAY['Ação', 'Aventura']
        
        -- EMOÇÃO E DRAMA (Mapeando Soap, Drama, Comédia, Romance)
        WHEN 'Comédia' THEN ARRAY['Comédia']
        WHEN 'Drama' THEN ARRAY['Drama']
        WHEN 'Romance' THEN ARRAY['Romance']
        WHEN 'Soap' THEN ARRAY['Drama', 'Romance'] -- Novelas são puro drama e romance
        
        -- FANTASIA E SCI-FI (Sci-Fi & Fantasy Agrupados)
        WHEN 'Sci-Fi & Fantasy' THEN ARRAY['Ficção Científica', 'Fantasia']
        WHEN 'Animação' THEN ARRAY['Infantil / Família', 'Fantasia']
        
        -- INFANTIL (Kids e Família)
        WHEN 'Kids' THEN ARRAY['Infantil / Família']
        WHEN 'Família' THEN ARRAY['Infantil / Família']
        
        -- TENSÃO E MISTÉRIO (Crime e Mistério)
        WHEN 'Mistério' THEN ARRAY['Terror / Suspense', 'Estratégia / Raciocínio']
        WHEN 'Crime' THEN ARRAY['Terror / Suspense', 'Drama']
        
        -- REALIDADE, POLÍTICA E TV (News, Talk, Reality, War & Politics)
        WHEN 'Documentário' THEN ARRAY['Realidade / Educação']
        WHEN 'História' THEN ARRAY['Realidade / Educação', 'Drama']
        WHEN 'News' THEN ARRAY['Realidade / Educação']
        WHEN 'Talk' THEN ARRAY['Realidade / Educação', 'Comédia'] -- Talk shows variam, mas isso cobre bem
        WHEN 'Reality' THEN ARRAY['Realidade / Educação', 'Comédia']
        WHEN 'War & Politics' THEN ARRAY['Ação', 'Drama', 'Estratégia / Raciocínio'] -- Adicionei estratégia por causa de política
        
        ELSE ARRAY[]::text[]
      END as mapped_array
    FROM unnest(generos) as g
  ) sub_map,
  unnest(sub_map.mapped_array) as unnest_super_genre
)
WHERE categoria = 'serie';