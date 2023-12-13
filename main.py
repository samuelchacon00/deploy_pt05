import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from fastapi import FastAPI

def set_year(string):    
    if string=="-1":
        return -1
    string=string.split("-")

    return int(string[0])

def isfloat(valor):
    try:
        float(valor)
        return True
    except:
        return False

def generar_unicos(df,campo):
    unicos=[]
    for i in df.index:
        for n in df.at[i,campo]:
            if n not in unicos:
                unicos.append(n)

    dicc={}
    for i in range(len(unicos)):
        dicc[unicos[i]]=i

    return dicc

def normalizar(lista,unicos):
    norma=[]
    for i in range(len(lista)):
        norma.append(unicos[lista[i]])
    
    return norma

app = FastAPI()

data_dir="datasets/"

games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
reviews=pd.read_csv(data_dir+"user_reviews.csv",sep=";")
items=pd.read_parquet(data_dir+"users_items.parquet")

@app.get("/PlayTimeGenre/{genero}")
def PlayTimeGenre(genero:str):
    items["item_id"]=items["item_id"].apply(lambda x:int(x))
    tabla=pd.merge(items[["item_id","playtime_forever"]],games[["item_id","genres","tags","release_date"]],on="item_id",how="inner")
    mask=(tabla["genres"].apply(lambda x:genero.lower() in x.lower()) | tabla["tags"].apply(lambda x:genero.lower() in x.lower()))

    tabla["year"]=tabla["release_date"].apply(lambda x:set_year(x))
    indices=tabla[mask][["year","playtime_forever"]].groupby("year").sum("playtime_forever").sort_values(by="playtime_forever",ascending=False).index
    
    if not len(indices):
        print(indices)
        return {"No se econtro el genero":-1}
    
    
    return {"Año de lanzamiento con más horas jugadas para Género "+genero : int(indices[0])}

@app.get("/UserForGenre/{genero}")
def UserForGenre(genero:str):

    tabla=pd.merge(items[["user_id","item_id","playtime_forever"]],games[["item_id","genres","tags","release_date"]],on="item_id",how="inner")
    mask=(tabla["genres"].apply(lambda x:genero.lower() in x.lower()) | tabla["tags"].apply(lambda x:genero.lower() in x.lower()))
    tabla["year"]=tabla["release_date"].apply(lambda x:set_year(x))

    tabla=tabla[mask][["year","user_id","playtime_forever"]].groupby(["user_id","year"])["playtime_forever"].sum().reset_index()

    usuario=tabla.groupby("user_id")["playtime_forever"].sum().reset_index().sort_values(by="playtime_forever",ascending=False)["user_id"][0]

    mask=(tabla["user_id"]==usuario) & ~(tabla["year"]==-1)
    tabla=tabla[mask]

    dicc={"Usuarios con más horas jugadas para Género X"+genero : usuario,"Horas jugadas":[]}

    for i in tabla.index:
        dicc["Horas jugadas"].append({"Año":int(tabla.at[i,"year"]),"Horas":int(tabla.at[i,"playtime_forever"])})
    
    if not len(dicc["Horas jugadas"]):
        return {"No se econtro el genero":-1}
    return dicc

@app.get("/UsersRecommend/{year}")
def UsersRecommend(year:int):
    mask=~(reviews["posted"]=="Unknow")
    #print(reviews.isna().sum())
    reviews["year"]=reviews[mask]["posted"].apply(lambda x:int(x.split("/")[2]))
    reviews["year"].fillna(-1,inplace=True)
    reviews["year"]=reviews["year"].apply(lambda x:int(x))

    mask=(reviews["sentiment_analysis"].apply(lambda x:x in [1,2])) & (reviews["year"]==year)
    tabla=reviews[mask][["item_id","recommend","sentiment_analysis"]].groupby(["item_id"]).sum("recommend").sort_values(by="recommend",ascending=False).reset_index()
    if not len(tabla):
        return {"No se encontro el año":-1}
    
    inter=pd.merge(games[["item_id","app_name"]],tabla,on="item_id",how="inner")
    inter=inter.sort_values(by="recommend",ascending=False).reset_index()["app_name"][:3]

    puesto="Puesto X"
    lista=[]
    for i in range(3):
        lista.append({puesto.replace("X",str(i+1)):inter[i]})

    return lista

@app.get("/UsersNotRecommend/{year}")
def UsersNotRecommend(year:int):
    mask=~(reviews["posted"]=="Unknow")
    #print(reviews.isna().sum())
    
    reviews["year"]=reviews[mask]["posted"].apply(lambda x:int(x.split("/")[2]))
    reviews["year"].fillna(-1,inplace=True)
    reviews["year"]=reviews["year"].apply(lambda x:int(x))

    mask=(reviews["sentiment_analysis"].apply(lambda x:x==0)) & (reviews["year"]==year) & (reviews["recommend"].apply(lambda x: not x))
    tabla=reviews[mask][["item_id","year","recommend"]]
    
    if not len(tabla):
        return {"No se encontro el año":-1}
    
    tabla["recommend"]=tabla["recommend"].apply(lambda x:not x)

    inter=pd.merge(games[["item_id","app_name"]],tabla,on="item_id",how="inner")
    inter=inter.sort_values(by="recommend",ascending=False).reset_index()["app_name"][:3]

    puesto="Puesto X"
    lista=[]
    for i in range(3):
        lista.append({puesto.replace("X",str(i+1)):inter[i]})

    return lista

@app.get("/sentiment_analysis/{year}")
def sentiment_analysis(year:int):
    mask=~(reviews["posted"]=="Unknow")

    reviews["year"]=reviews[mask]["posted"].apply(lambda x:int(x.split("/")[2]))
    reviews["year"].fillna(-1,inplace=True)
    reviews["year"]=reviews["year"].apply(lambda x:int(x))

    mask=reviews["year"]==year

    tabla=reviews[mask]["sentiment_analysis"].value_counts().reset_index()

    if not len(tabla):
        return {"No se encontro el año":-1}    

    return {"Negative":int(tabla[tabla["sentiment_analysis"]==0].reset_index()["count"][0]),
            "Neutral":int(tabla[tabla["sentiment_analysis"]==1].reset_index()["count"][0]),
            "Positive":int(tabla[tabla["sentiment_analysis"]==2].reset_index()["count"][0])}

@app.get("/recomendacion_juego/{id}")
def recomendacion_juego(id:int):
    mask=games["item_id"]==id
    if not len(games[mask]):
        return {"No se encontro el id":-1}
    
    df_genres = games.explode('genres')[['genres']]
    df_tags = games.explode('tags')[['tags']]

    df_genres_encoded = pd.get_dummies(df_genres['genres']).groupby(df_genres.index).max()
    df_tags_encoded = pd.get_dummies(df_tags['tags']).groupby(df_tags.index).max()

    tabla = pd.concat([df_genres_encoded, df_tags_encoded], axis=1)

    similarities=cosine_similarity(tabla)
    
    indice_juego = games[mask].index[0]

    similitudes_con_juego = similarities[indice_juego]

    indices_top_similares = similitudes_con_juego.argsort()[::-1][1:5+1]
    
    juegos_recomendados = [games.at[i,"app_name"] for i in indices_top_similares]

    return juegos_recomendados

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
