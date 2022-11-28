from suds.client import Client

def test_Recup_Auteur(id_publication):
   Recup_Auteur_client = Client("http://127.0.0.1:8000/Recup_Auteur_Service?wsdl")
   print(Recup_Auteur_client.service.Recup_Auteur(id_publication))
def test_Recup_Hashtags(id_publication):
   Recup_Hashtags_client = Client("http://127.0.0.1:8000/Recup_Hashtags_Service?wsdl")
   print(Recup_Hashtags_client.service.Recup_Hashtags(id_publication))
def test_Analyse_Sentiment(id_publication):
   Analyse_Sentiment_client = Client("http://127.0.0.1:8000/Analyse_Sentiment_Service?wsdl")
   print(Analyse_Sentiment_client.service.Analyse_Sentiment(id_publication))
def test_Identification_Topics(id_publication):
    Identification_Topics_client = Client("http://127.0.0.1:8000/Identification_Topics_Service?wsdl")
    print(Identification_Topics_client.service.Identification_Topics(id_publication))
def test_Traitement_Donnees(id_publication):
    Traitement_Donnees_client = Client("http://127.0.0.1:8000/Traitement_Donnees_Service?wsdl")
    print(Traitement_Donnees_client.service.Traitement_Donnees(id_publication))
def test_Top_K_utilisateurs(K):
    Top_K_utilisateurs_client = Client("http://127.0.0.1:8000/Top_K_utilisateurs_Service?wsdl")
    print(Top_K_utilisateurs_client.service.Top_K_utilisateurs(K))
def test_Top_K_Hashtags(K):
    Top_K_Hashtags_client = Client("http://127.0.0.1:8000/Top_K_Hashtags_Service?wsdl")
    print(Top_K_Hashtags_client.service.Top_K_Hashtags(K))
def test_Top_K_Topics(K):
    Top_K_Topics_client = Client("http://127.0.0.1:8000/Top_K_Topics_Service?wsdl")
    print(Top_K_Topics_client.service.Top_K_Topics(K))
def test_nombre_de_publications_par_utilisateur():
    nombre_de_publications_par_utilisateur_client = Client("http://127.0.0.1:8000/nombre_de_publications_par_utilisateur_Service?wsdl")
    print(nombre_de_publications_par_utilisateur_client.service.nombre_de_publications_par_utilisateur())
def test_nombre_de_publications_par_hashtags():
    nombre_de_publications_par_hashtags_client = Client("http://127.0.0.1:8000/nombre_de_publications_par_hashtags_Service?wsdl")
    print(nombre_de_publications_par_hashtags_client.service.nombre_de_publications_par_hashtags())
def test_nombre_de_publications_par_topics():
    nombre_de_publications_par_topics_client = Client("http://127.0.0.1:8000/nombre_de_publications_par_topics_Service?wsdl")
    print(nombre_de_publications_par_topics_client.service.nombre_de_publications_par_topics())
def test_Analyse_Donnees(K):
    Analyse_Donnees_client = Client("http://127.0.0.1:8000/Analyse_Donnees_Service?wsdl")
    print(Analyse_Donnees_client.service.Analyse_Donnees(K))

if __name__ == '__main__':
    # test_Recup_Auteur("1421599703116943360")
    # test_Recup_Hashtags("1421599703116943360")
    # test_Analyse_Sentiment("1421599703116943360")
    # test_Identification_Topics("1421599703116943360")
    #
    # test_Traitement_Donnees("1421599703116943360")

    test_Top_K_utilisateurs(5)
    test_Top_K_Hashtags(5)
    test_Top_K_Topics(5)

    test_nombre_de_publications_par_utilisateur();
    test_nombre_de_publications_par_hashtags();
    test_nombre_de_publications_par_topics();

    print("########  ANALYSE DONNEES  ###########")
    test_Analyse_Donnees(5);