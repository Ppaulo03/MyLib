from filmes import buscar_e_salvar_filmes
from jogos import buscar_e_salvar_jogos
from livros import buscar_e_salvar_livros
from animes import buscar_e_salvar_animes

if __name__ == "__main__":
    print("Iniciando a população do banco de dados Supabase...")
    print("Populando filmes...")
    buscar_e_salvar_filmes()
    print("Populando jogos...")
    buscar_e_salvar_jogos()
    print("Populando livros...")
    buscar_e_salvar_livros()
    print("Populando animes...")
    buscar_e_salvar_animes()
    print("População concluída.")
