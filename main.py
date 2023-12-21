import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from fastapi import FastAPI
from datetime import datetime
import ast


def get_year_1(fecha):
    if isinstance(fecha,str):
        return datetime.strptime(fecha,"%d/%m/%Y").year
    else:
        return fecha
    
def get_year_2(fecha):
    if isinstance(fecha,str):
        return int(fecha.split("-")[0])
    else:
        return fecha
    
def definir_polaridad(valor,polaridad="negativa"):
    if polaridad == "negativa":
        if not valor:
            return 1
        else:
            return 0
    else:
        if valor in [1,2]:
            return 1
        else:
            return 0


app = FastAPI()

data_dir="datasets/"


#probar con Action
@app.get("/PlayTimeGenre/{genero}")
def PlayTimeGenre(genero:str):
    games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
    items=pd.read_parquet(data_dir+"users_items.parquet")

    games["item_id"]=games["id"].apply(lambda x:int(x))
    tabla=pd.merge(items[["item_id","playtime_forever"]],games[["item_id","genres","tags","release_date"]],on="item_id",how="inner")
    mask=(tabla["genres"].apply(lambda x:genero.lower() in x.lower()) | tabla["tags"].apply(lambda x:genero.lower() in x.lower()))

    tabla["year"]=tabla["release_date"].apply(lambda x:get_year_2(x))
    mask= mask & ~(tabla["year"].isna())
    indices=tabla[mask][["year","playtime_forever"]].groupby("year").sum("playtime_forever").sort_values(by="playtime_forever",ascending=False).index
    
    if not len(indices):
        return {"No se econtro el genero":-1}
    
    return {"Año de lanzamiento con más horas jugadas para Género "+genero : int(indices[0])}

#probar con adventure
@app.get("/UserForGenre/{genero}")
def UserForGenre(genero:str):
    games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
    items=pd.read_parquet(data_dir+"users_items.parquet")

    mask1=(games["genres"].apply(lambda x:genero.lower() in x.lower())) | (games["tags"].apply(lambda x:genero.lower() in x.lower()))
    games["item_id"]=games["id"]

    if not len(games[mask1]):
        return {"No se encontro en el genero":-1}

    games["year"]=games["release_date"].apply(lambda x:get_year_2(x))
    mask2=mask1 & ~(games["year"].isna())
    
    tabla=pd.merge(games[mask2][["year","item_id","release_date"]],items[["item_id","playtime_forever","user_id"]],on="item_id",how="right")
    tabla=tabla.groupby(["user_id","year"]).agg({"playtime_forever":"sum"}).reset_index().sort_values(by="playtime_forever",ascending=False).reset_index()
    
    if not len(tabla):
        return {"no se encontraron usuarios que jueguen el genero "+genero:-1}

    user=tabla.at[0,"user_id"]

    tabla=tabla[tabla["user_id"]==user].sort_values(by="year",ascending=False)

    dicc={"Usuario con más horas jugadas para Género X".replace("X",genero):user,"Horas jugadas":[]}    
    
    for i in tabla.index:
        dicc["Horas jugadas"].append({"Año":tabla.at[i,"year"],"Horas":tabla.at[i,"playtime_forever"]})

    return dicc

#probar con 2015
@app.get("/UsersRecommend/{year}")
def UsersRecommend(year:int):
    games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
    reviews=pd.read_csv(data_dir+"user_reviews.csv",sep=";")
    
    reviews["year"]=reviews["posted"].apply(lambda x:get_year_1(x))
    mask=reviews["year"]==year
    games["item_id"]=games["id"]
    games.drop(columns=["id"],inplace=True)
    tab=pd.merge(reviews[mask][["item_id","user_id","recommend","sentiment_analysis"]],games[["item_id","app_name"]],on="item_id",how="left")
    tab["negativos"]=tab["sentiment_analysis"].apply(lambda x:definir_polaridad(x,polaridad="positiva"))
    tab=list(tab.groupby(["app_name"]).agg({"recommend":"sum","negativos":"sum"}).reset_index().sort_values(by=["recommend","negativos"],ascending=False)["app_name"])
    
    text="Puesto X"
    lista=[]
    if len(tab)>=3:
        for i in range(3):
            lista.append({text.replace("X",str(i+1)):tab[i]})
    else:
        for i in range(tab):
            lista.append({text.replace("X",str(i+1)):tab[i]})

    return lista

#probar con 2015
@app.get("/UsersNotRecommend/{year}")
def UsersNotRecommend(year:int):
    games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
    reviews=pd.read_csv(data_dir+"user_reviews.csv",sep=";")
    
    reviews["year"]=reviews["posted"].apply(lambda x:get_year_1(x))
    mask=reviews["year"]==year
    games["item_id"]=games["id"]
    games.drop(columns=["id"],inplace=True)
    tab=pd.merge(reviews[mask][["item_id","user_id","recommend","sentiment_analysis"]],games[["item_id","app_name"]],on="item_id",how="left")
    tab["negativos"]=tab["sentiment_analysis"].apply(lambda x:definir_polaridad(x,polaridad="negativa"))
    tab["recommend"]=tab["recommend"].apply(lambda x:not x)
    tab=list(tab.groupby(["app_name"]).agg({"recommend":"sum","negativos":"sum"}).reset_index().sort_values(by=["recommend","negativos"],ascending=False)["app_name"])
    
    text="Puesto X"
    lista=[]
    if len(tab)>=3:
        for i in range(3):
            lista.append({text.replace("X",str(i+1)):tab[i]})
    else:
        for i in range(tab):
            lista.append({text.replace("X",str(i+1)):tab[i]})

    return lista

#probar con 2015
@app.get("/sentiment_analysis/{year}")
def sentiment_analysis(year:int):
    games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
    reviews=pd.read_csv(data_dir+"user_reviews.csv",sep=";")

    games["item_id"]=games["id"]
    games.drop(columns=["id"],inplace=True)
    games["year"]=games["release_date"].apply(lambda x:get_year_2(x))
    mask=games["year"]==year
    tabla=pd.merge(games[mask][["item_id"]],reviews[["item_id","sentiment_analysis"]],on="item_id",how="right")

    if not len(tabla):
        return {"No se encontro la año de lanzamiento":-1}    

    colum_name="sentiment_analysis"
 
    return {"Negative":int(tabla[tabla[colum_name]==0][colum_name].count()),
            "Neutral":int(tabla[tabla[colum_name]==1][colum_name].count()),
            "Positive":int(tabla[tabla[colum_name]==2][colum_name].count())}

#probar con 643980 o 220
@app.get("/recomendacion_juego/{id}")
def recomendacion_juego(id:int):
    games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
    mask=games["id"]==id

    if not len(games[mask]):
        return {"No se encontro el id":-1}

    games["tags"]=games["tags"].apply(lambda x:ast.literal_eval(x))
    games["genres"]=games["genres"].apply(lambda x:ast.literal_eval(x))
    df_genres=games.explode('genres')[['genres']]
    df_tags=games.explode('tags')[['tags']]
    df_genres_encoded=pd.get_dummies(df_genres['genres']).groupby(df_genres.index).max()
    df_tags_encoded=pd.get_dummies(df_tags['tags']).groupby(df_tags.index).max()
    tabla=pd.concat([df_genres_encoded, df_tags_encoded], axis=1)
    similitudes=cosine_similarity(tabla)
    indice_juego=games[mask].index[0]
    similitudes_con_juego=similitudes[indice_juego]
    indices_top_similares=similitudes_con_juego.argsort()[::-1][1:5+1]
    juegos_recomendados=[games.at[i,"app_name"] for i in indices_top_similares]

    return juegos_recomendados
