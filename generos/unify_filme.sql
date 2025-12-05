UPDATE midia
SET generos_unificados = ARRAY(
  SELECT DISTINCT unnest_super_genre
  FROM (
    SELECT
      CASE TRIM(g)
        -- AÇÃO E AVENTURA
        WHEN 'Ação' THEN ARRAY['Ação']
        WHEN 'Aventura' THEN ARRAY['Aventura']
        WHEN 'Faroeste' THEN ARRAY['Ação', 'Aventura']
        WHEN 'Guerra' THEN ARRAY['Ação', 'Drama']
        
        -- EMOÇÃO E DRAMA
        WHEN 'Comédia' THEN ARRAY['Comédia']
        WHEN 'Drama' THEN ARRAY['Drama']
        WHEN 'Romance' THEN ARRAY['Romance']
        WHEN 'Cinema TV' THEN ARRAY['Drama', 'Romance'] -- Geralmente são filmes leves de TV
        
        -- FANTASIA E INFANTIL
        WHEN 'Fantasia' THEN ARRAY['Fantasia']
        WHEN 'Ficção científica' THEN ARRAY['Ficção Científica']
        WHEN 'Animação' THEN ARRAY['Infantil / Família', 'Fantasia'] -- Ponte com Jogos/Animes
        WHEN 'Família' THEN ARRAY['Infantil / Família']
        
        -- TENSÃO E MISTÉRIO
        WHEN 'Terror' THEN ARRAY['Terror / Suspense']
        WHEN 'Thriller' THEN ARRAY['Terror / Suspense']
        WHEN 'Mistério' THEN ARRAY['Terror / Suspense', 'Estratégia / Raciocínio']
        WHEN 'Crime' THEN ARRAY['Terror / Suspense', 'Drama']
        
        -- REALIDADE E CULTURA
        WHEN 'Documentário' THEN ARRAY['Realidade / Educação']
        WHEN 'História' THEN ARRAY['Realidade / Educação', 'Drama']
        WHEN 'Música' THEN ARRAY['Música']
        
        ELSE ARRAY[]::text[]
      END as mapped_array
    FROM unnest(generos) as g
  ) sub_map,
  unnest(sub_map.mapped_array) as unnest_super_genre
)
WHERE categoria = 'filme';