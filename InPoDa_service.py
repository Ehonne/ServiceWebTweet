import logging
import sys
import re
import random

import pymongo
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer

logging.basicConfig(level=logging.DEBUG)

from spyne import Application, rpc, ServiceBase, \
    Integer, Unicode, Array, AnyDict

from spyne import Iterable
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication

from spyne.util.wsgi_wrapper import run_twisted


class Recup_Auteur_Service(ServiceBase):
    @rpc(Unicode, _returns=Unicode)
    def Recup_Auteur(ctx, id_publication):
        result = client["SOA"]["TWEETS"].find_one({"id":id_publication}, {"_id": 0, "text": 1, "author_id": 1})
        return result.get("author_id")

class Recup_Hashtags_Service(ServiceBase):
    @rpc(Unicode, _returns=Array(Unicode))
    def Recup_Hashtags(ctx, id_publication):
        result = client["SOA"]["TWEETS"].find_one({"id":id_publication}, {"_id": 0, "text": 1, "author_id": 1})
        publication = result.get("text")
        Hashtags = re.findall(r"(#+[a-zA-Z0-9(_)]{1,})", publication)
        return Hashtags

class Analyse_Sentiment_Service(ServiceBase):
    @rpc(Unicode, _returns=float)
    def Analyse_Sentiment(ctx, id_publication):
        result = client["SOA"]["TWEETS"].find_one({"id":id_publication}, {"_id": 0, "text": 1, "author_id": 1})
        publication = result.get("text")
        blob = TextBlob(publication,pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())
        return blob.sentiment[0]

class Identification_Topics_Service(ServiceBase):
    @rpc(Unicode, _returns=Array(Unicode))
    def Identification_Topics(ctx, id_publication):
        result = client["SOA"]["TWEETS"].find_one({"id":id_publication}, {"_id": 0, "text": 1, "author_id": 1})
        Topics = ["Politique", "Scientifique", "Finance", "Santé", "Culture", "People", "Sport"]
        Pics = random.sample(Topics, random.randint(1, 4))
        return Pics

class Traitement_Donnees_Service(ServiceBase):
    @rpc(Unicode, _returns=Unicode)
    def Traitement_Donnees(ctx, id_publication):
        result = client["SOA"]["TWEETS"].find_one({"id":id_publication}, {"_id": 0, "text": 1, "author_id": 1})
        auteur = result.get("author_id")
        Hashtags = Recup_Hashtags_Service.Recup_Hashtags(ctx,id_publication)
        Feel = Analyse_Sentiment_Service.Analyse_Sentiment(ctx,id_publication)
        Pics = Identification_Topics_Service.Identification_Topics(ctx,id_publication)
        key = {"Publication": id_publication}
        data = {"Publication": id_publication, "Auteur": auteur, "Hashtags": Hashtags, "Sentiments": Feel,
                "Topics": Pics};
        client["SOA"]["TraitementDATA"].update_one(key, {"$setOnInsert": data}, upsert=True)
        return "Auteur: " + auteur+"\n"+"Hashtags: "+', '.join(Hashtags)+"\n"+"Topics: "+', '.join(Pics)+"\n"+"Sentiments: "+str(Feel)

class Top_K_Hashtags_Service(ServiceBase):
    @rpc(int, _returns=Array(AnyDict))
    def Top_K_Hashtags(ctx, K):
        Top_k_Hashtag = client["SOA"]["TraitementDATA"].aggregate([{"$unwind": "$Hashtags"},
                                                                  {"$group": {"_id": "$Hashtags",
                                                                              "Ocurence": {"$sum": 1}}},
                                                                  {"$project": {"_id": 0, "Ocurence": 1,
                                                                                "Hashtags": "$_id"}},
                                                                  {"$sort": {"Ocurence": -1}}, {"$limit": K}])
        liste = [val for val in Top_k_Hashtag]
        return liste

class Top_K_utilisateurs_Service(ServiceBase):
    @rpc(int, _returns=Array(AnyDict))
    def Top_K_utilisateurs(ctx, K):
        Top_k_auteur = client["SOA"]["TraitementDATA"].aggregate([
            {"$group": {"_id": "$Auteur", "NB_Tweets": {"$sum": 1}}}, {"$sort": {"NB_Tweets": -1}}, {"$limit": K}])
        liste = [val for val in Top_k_auteur]
        return liste

class Top_K_Topics_Service(ServiceBase):
    @rpc(int, _returns=Array(AnyDict))
    def Top_K_Topics(ctx, K):
        Top_k_Topicss = client["SOA"]["TraitementDATA"].aggregate([{"$unwind": "$Topics" },
            {"$group": { "_id": "$Topics", "Ocurence": { "$sum": 1 } }},
            {"$project": { "_id": 0, "Ocurence": 1,"Topics": "$_id" } },
            {"$sort": { "Ocurence": -1 } }, {"$limit": K}])
        liste = [val for val in Top_k_Topicss]
        return liste

class  nombre_de_publications_par_utilisateur_Service(ServiceBase):
    @rpc(_returns=Array(AnyDict))
    def nombre_de_publications_par_utilisateur(ctx):
        nombre_de_publications_par_utilisateurs = client["SOA"]["TraitementDATA"].aggregate([
            {"$group": {"_id": "$Auteur", "NB_Tweets": {"$sum": 1}}},{"$project": { "_id": 0, "Auteur": "$_id" ,"NB_Tweets": 1} } ])
        liste = [val for val in nombre_de_publications_par_utilisateurs]
        return liste

class  nombre_de_publications_par_hashtags_Service(ServiceBase):
    @rpc(_returns=Array(AnyDict))
    def nombre_de_publications_par_hashtags(ctx):
        nombre_de_publications_par_hashtag = client["SOA"]["TraitementDATA"].aggregate([{"$unwind": "$Hashtags"},
                                                                  {"$group": {"_id": "$Hashtags",
                                                                              "Ocurence": {"$sum": 1}}},
                                                                  {"$project": {"_id": 0, "Ocurence": 1,
                                                                                "Hashtags": "$_id"}},
                                                                  ])
        liste = [val for val in nombre_de_publications_par_hashtag]
        return liste

class  nombre_de_publications_par_topics_Service(ServiceBase):
    @rpc(_returns=Array(AnyDict))
    def nombre_de_publications_par_topics(ctx):
        nombre_de_publications_par_topicss = client["SOA"]["TraitementDATA"].aggregate([{"$unwind": "$Topics" },
            {"$group": { "_id": "$Topics", "Ocurence": { "$sum": 1 } }},
            {"$project": { "_id": 0, "Ocurence": 1,"Topics": "$_id" } }])
        liste = [val for val in nombre_de_publications_par_topicss]
        return liste

class  Analyse_Donnees_Service(ServiceBase):
    @rpc(int,_returns=Array(AnyDict))
    def Analyse_Donnees(ctx, K):
        listeKuser = Top_K_utilisateurs_Service.Top_K_utilisateurs(ctx,K)
        listeKHashtags = Top_K_Hashtags_Service.Top_K_Hashtags(ctx,K)
        listeKTopics = Top_K_Topics_Service.Top_K_Topics(ctx,K)
        listeNBRUser = nombre_de_publications_par_utilisateur_Service.nombre_de_publications_par_utilisateur(ctx)
        listeNBRHashtags = nombre_de_publications_par_hashtags_Service.nombre_de_publications_par_hashtags(ctx)
        listeNBRTopics = nombre_de_publications_par_topics_Service.nombre_de_publications_par_topics(ctx)
        liste = listeKuser + listeKHashtags + listeKTopics + listeNBRUser + listeNBRHashtags + listeNBRTopics
        return liste

application = Application([Recup_Auteur_Service],
                          tns='spyne.examples.Recup_Auteur_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )
application2 = Application([Recup_Hashtags_Service],
                          tns='spyne.examples.Recup_Hashtags_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )
application3 = Application([Analyse_Sentiment_Service],
                          tns='spyne.examples.Analyse_Sentiment_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )
application4 = Application([Identification_Topics_Service],
                          tns='spyne.examples.Identification_Topics_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )
application5 = Application([Traitement_Donnees_Service],
                          tns='spyne.examples.Traitement_Donnees_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )
application6 = Application([Top_K_utilisateurs_Service],
                          tns='spyne.examples.Top_K_utilisateurs_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )
application7 = Application([Top_K_Hashtags_Service],
                          tns='spyne.examples.Top_K_Hashtags_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )
application8 = Application([Top_K_Topics_Service],
                          tns='spyne.examples.Top_K_Topics_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )
application9 = Application([nombre_de_publications_par_utilisateur_Service],
                          tns='spyne.examples.nombre_de_publications_par_utilisateur_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )
application10 = Application([nombre_de_publications_par_hashtags_Service],
                          tns='spyne.examples.nombre_de_publications_par_hashtags_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )
application11 = Application([nombre_de_publications_par_topics_Service],
                          tns='spyne.examples.nombre_de_publications_par_topics_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )
application12 = Application([Analyse_Donnees_Service],
                          tns='spyne.examples.Analyse_Donnees_Service',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11()
                          )

if __name__ == '__main__':
    uri = 'mongodb+srv://admin:uvsqawsgroupe17@cluster0.nkdni.mongodb.net/?retryWrites=true&w=majority'
    client = pymongo.MongoClient(uri)
    wsgi_app = WsgiApplication(application)
    wsgi_app2 = WsgiApplication(application2)
    wsgi_app3 = WsgiApplication(application3)
    wsgi_app4 = WsgiApplication(application4)
    wsgi_app5 = WsgiApplication(application5)
    wsgi_app6 = WsgiApplication(application6)
    wsgi_app7 = WsgiApplication(application7)
    wsgi_app8 = WsgiApplication(application8)
    wsgi_app9 = WsgiApplication(application9)
    wsgi_app10 = WsgiApplication(application10)
    wsgi_app11 = WsgiApplication(application11)
    wsgi_app12 = WsgiApplication(application12)

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('spyne.protocol.xml').setLevel(logging.DEBUG)

    logging.info("listening to http://127.0.0.1:8000/%22")
    logging.info("wsdl is at: http://localhost:8000/?wsdl%22")

    # server = make_server('127.0.0.1', 8000, wsgi_app)
    # server.serve_forever()
    twisted_apps = [(wsgi_app, b'Recup_Auteur_Service'),
                    (wsgi_app2, b'Recup_Hashtags_Service'),
                    (wsgi_app3, b'Analyse_Sentiment_Service'),
                    (wsgi_app4, b'Identification_Topics_Service'),
                    (wsgi_app5, b'Traitement_Donnees_Service'),
                    (wsgi_app6, b'Top_K_utilisateurs_Service'),
                    (wsgi_app7, b'Top_K_Hashtags_Service'),
                    (wsgi_app8, b'Top_K_Topics_Service'),
                    (wsgi_app9, b'nombre_de_publications_par_utilisateur_Service'),
                    (wsgi_app10, b'nombre_de_publications_par_hashtags_Service'),
                    (wsgi_app11, b'nombre_de_publications_par_topics_Service'),
                    (wsgi_app12, b'Analyse_Donnees_Service')
                    ]

    sys.exit(run_twisted(twisted_apps, 8000)),