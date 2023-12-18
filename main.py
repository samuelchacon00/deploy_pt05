import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from fastapi import FastAPI
<<<<<<< HEAD
from datetime import datetime
import ast
=======
>>>>>>> a77b61875f10ffc75cfb30a1b17acfa2309f9545

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

def get_year_1(fecha):
    if isinstance(fecha,str):
        return datetime.strptime(fecha,"%d/%m/%Y").year
    else:
        return fecha
    
def get_year_2(fecha):
    if isinstance(fecha,str):
        return fecha.split("-")[0]
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

games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
reviews=pd.read_csv(data_dir+"user_reviews.csv",sep=";")
items=pd.read_parquet(data_dir+"users_items.parquet")

<<<<<<< HEAD
#probar con Action
=======
>>>>>>> a77b61875f10ffc75cfb30a1b17acfa2309f9545
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

<<<<<<< HEAD
#probar con adventure
=======
>>>>>>> a77b61875f10ffc75cfb30a1b17acfa2309f9545
@app.get("/UserForGenre/{genero}")
def UserForGenre(genero:str):
    games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
    items=pd.read_parquet(data_dir+"users_items.parquet")

    mask1=(games["genres"].apply(lambda x:genero.lower() in x.lower())) | (games["tags"].apply(lambda x:genero.lower() in x.lower()))
    games["item_id"]=games["id"]

    games["year"]=games["release_date"].apply(lambda x:get_year_2(x))
    mask2=mask1 & (~games["release_date"].isna())
    
    tabla=pd.merge(games[mask2][["year","item_id","release_date"]],items[["item_id","playtime_forever","user_id"]],on="item_id",how="inner")
    b_user=tabla.groupby(["user_id"]).agg({"playtime_forever":"sum"}).reset_index()
    usuario=b_user["user_id"][0]

<<<<<<< HEAD
    respuesta={"Usuario con más horas jugadas para Género X".replace("X",genero):usuario}

    mask=tabla["user_id"]==usuario
    tabla=tabla[mask].groupby(["year"]).agg({"playtime_forever":"sum"}).sort_values(by="year",ascending=False).reset_index()

    lista=[]
    for i in tabla.index:
        lista.append({"Año":tabla.at[i,"year"],"Horas":tabla.at[i,"playtime_forever"]})

    respuesta["Horas jugadas"]=lista

    return respuesta

#probar con 2015
=======
>>>>>>> a77b61875f10ffc75cfb30a1b17acfa2309f9545
@app.get("/UsersRecommend/{year}")
def UsersRecommend(year:int):
    games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
    reviews=pd.read_csv(data_dir+"user_reviews.csv",sep=";")

    reviews["year"]=reviews["posted"].apply(lambda x:get_year_1(x))
    mask=reviews["year"]==year
    games["item_id"]=games["id"]
    reviews["positivos"]=reviews["sentiment_analysis"].apply(lambda x:definir_polaridad(x,"positiva"))
    tabla=pd.merge(reviews[mask][["year","item_id","positivos","recommend"]],games[["item_id","app_name"]],on="item_id",how="inner")
    
<<<<<<< HEAD
    tabla=tabla.groupby(["year","app_name"]).agg({"recommend":"sum","positivos":"sum"}).reset_index().sort_values(by=["positivos","recommend"],ascending=False).reset_index()
=======
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
>>>>>>> a77b61875f10ffc75cfb30a1b17acfa2309f9545
    
    top=[]
    text="Puesto X"
    if(len(tabla)>=3):
        for i in range(0,3):
            top.append({text.replace("X",str(i+1),):tabla.at[i,"app_name"]})
    else:
        for i in range(0,len(tabla)):
            top.append({text.replace("X",str(i+1),):tabla.at[i,"app_name"]})

    return top

#probar con 2015
@app.get("/UsersWorstDeveloper/{year}")
def UsersWorstDeveloper(year:int):
    games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
    reviews=pd.read_csv(data_dir+"user_reviews.csv",sep=";")

<<<<<<< HEAD
    games["item_id"]=games["id"]
=======
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
>>>>>>> a77b61875f10ffc75cfb30a1b17acfa2309f9545

    reviews["year"]=reviews["posted"].apply(lambda x:get_year_1(x))
    mask=reviews["year"]==year

    tabla=pd.merge(games[["item_id","app_name","developer"]],reviews[mask][["item_id","recommend","sentiment_analysis","year"]],on="item_id",how="inner")
    tabla["negativos"]=tabla["sentiment_analysis"].apply(lambda x:definir_polaridad(x))
    tabla["recommend"]=~tabla["recommend"]
    tabla=tabla.groupby(["developer"]).agg({"negativos":"sum","recommend":"sum"}).reset_index().sort_values(by="negativos",ascending=False)
    text="Puesto X"
    top=[]

    for i in range(0,3):
        top.append({text.replace("X",str(i+1)):tabla.at[i,"developer"]})
    
    return top

#probar con ubisoft
@app.get("/sentiment_analysis/{year}")
def sentiment_analysis(developer:str):
    games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
    reviews=pd.read_csv(data_dir+"user_reviews.csv",sep=";")

    games["item_id"]=games["id"]
    games.drop(columns=["id"],inplace=True)
    mask=games["developer"].apply(lambda x:x.lower()==developer.lower())

    tabla=pd.merge(games[mask][["developer","item_id"]],reviews[["item_id","sentiment_analysis"]],on="item_id",how="inner")
    
    if not len(tabla):
        return {"No se encontro la empresa desarrolladora":-1}    

    colum_name="sentiment_analysis"
 
    return {"Negative":int(tabla[tabla[colum_name]==0][colum_name].count()),
            "Neutral":int(tabla[tabla[colum_name]==1][colum_name].count()),
            "Positive":int(tabla[tabla[colum_name]==2][colum_name].count())}

<<<<<<< HEAD
#probar con 80
=======
>>>>>>> a77b61875f10ffc75cfb30a1b17acfa2309f9545
@app.get("/recomendacion_juego/{id}")
def recomendacion_juego(id:int):
    games=pd.read_csv(data_dir+"steam_games.csv",sep=";")
    
    mask=games["id"]==id

    if not len(games[mask]):
        return {"No se encontro el id":-1}

    games["tags"]=games["tags"].apply(lambda x:ast.literal_eval(x))
    games["genres"]=games["genres"].apply(lambda x:ast.literal_eval(x))

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

<<<<<<< HEAD
print(PlayTimeGenre("freesdgf"))
print(UserForGenre("free to play"))
print(UsersRecommend(2015))
print(UsersWorstDeveloper(2015))
print(sentiment_analysis("ubisoft"))
print(recomendacion_juego(80))
=======
"""if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)"""
>>>>>>> a77b61875f10ffc75cfb30a1b17acfa2309f9545
