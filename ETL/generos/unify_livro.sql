UPDATE midia
SET generos_unificados = ARRAY(
  SELECT DISTINCT unnest_super_genre
  FROM (
    SELECT
      CASE TRIM(g)
        -- AVENTURA E FANTASIA
        WHEN 'Aventura' THEN ARRAY['Aventura']
        WHEN 'Fantasia' THEN ARRAY['Fantasia']
        WHEN 'Distopia' THEN ARRAY['Ficção Científica', 'Drama']
        WHEN 'HQs e Mangás' THEN ARRAY['Aventura', 'Fantasia', 'Ação'] -- Mangás variam, mas esse é o combo "shonen" padrão
        
        -- ROMANCE E DRAMA
        WHEN 'Romance' THEN ARRAY['Romance']
        WHEN 'Chick-lit' THEN ARRAY['Romance', 'Comédia']
        WHEN 'Jovem Adulto' THEN ARRAY['Drama', 'Romance'] -- Foco em relações e amadurecimento
        WHEN 'LGBTQIA+' THEN ARRAY['Romance', 'Drama']
        WHEN 'Contos' THEN ARRAY['Drama']
        WHEN 'Poesia' THEN ARRAY['Drama'] -- Arte focada em emoção
        
        -- TENSÃO E MISTÉRIO
        WHEN 'Terror' THEN ARRAY['Terror / Suspense']
        WHEN 'Suspense' THEN ARRAY['Terror / Suspense']
        WHEN 'Crime' THEN ARRAY['Terror / Suspense', 'Drama']
        WHEN 'Policial' THEN ARRAY['Terror / Suspense', 'Estratégia / Raciocínio'] -- Conecta com jogos de Puzzle
        
        -- COMÉDIA
        WHEN 'Humor' THEN ARRAY['Comédia']
        
        -- INFANTIL
        WHEN 'Infantil' THEN ARRAY['Infantil / Família']
        WHEN 'Infantojuvenil' THEN ARRAY['Infantil / Família', 'Aventura']
        
        -- NÃO-FICÇÃO / EDUCAÇÃO (O Grande Grupo)
        -- Todos estes conectam com Documentários ou Jogos de Simulação
        WHEN 'Biografia e Memórias' THEN ARRAY['Realidade / Educação', 'Drama']
        WHEN 'Viagens' THEN ARRAY['Realidade / Educação', 'Aventura']
        WHEN 'Autoajuda' THEN ARRAY['Realidade / Educação']
        WHEN 'Filosofia' THEN ARRAY['Realidade / Educação']
        WHEN 'Sociologia' THEN ARRAY['Realidade / Educação']
        WHEN 'Psicologia' THEN ARRAY['Realidade / Educação']
        WHEN 'Religião e Espiritualidade' THEN ARRAY['Realidade / Educação']
        WHEN 'Esoterismo' THEN ARRAY['Realidade / Educação']
        WHEN 'História' THEN ARRAY['Realidade / Educação', 'Drama']
        WHEN 'Política' THEN ARRAY['Realidade / Educação']
        WHEN 'Direito' THEN ARRAY['Realidade / Educação']
        WHEN 'Economia' THEN ARRAY['Realidade / Educação']
        WHEN 'Negócios' THEN ARRAY['Realidade / Educação']
        WHEN 'Marketing' THEN ARRAY['Realidade / Educação']
        WHEN 'Tecnologia' THEN ARRAY['Realidade / Educação', 'Ficção Científica']
        WHEN 'Ciências Biológicas' THEN ARRAY['Realidade / Educação']
        WHEN 'Saúde' THEN ARRAY['Realidade / Educação']
        WHEN 'Saúde e Bem-estar' THEN ARRAY['Realidade / Educação']
        WHEN 'Culinária' THEN ARRAY['Realidade / Educação']
        WHEN 'Arquitetura' THEN ARRAY['Realidade / Educação']
        WHEN 'Design' THEN ARRAY['Realidade / Educação']
        WHEN 'Artes' THEN ARRAY['Realidade / Educação']
        WHEN 'Fotografia' THEN ARRAY['Realidade / Educação']
        WHEN 'Moda' THEN ARRAY['Realidade / Educação']
        WHEN 'Educação' THEN ARRAY['Realidade / Educação']
        
        ELSE ARRAY[]::text[]
      END as mapped_array
    FROM unnest(generos) as g
  ) sub_map,
  unnest(sub_map.mapped_array) as unnest_super_genre
)
WHERE categoria = 'livro';