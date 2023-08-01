import json
import psycopg2
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from scipy.spatial.distance import cdist
from sklearn.preprocessing import MinMaxScaler



# Acessar banco de dados
import pandas as pd
import psycopg2


def execute_query(df):
    # Configuração de conexão
    connection_string = 'postgres://tabuada_cluster_teste_user:hm2itcVVmeYdya6KoFeisFXbqTpUBua7@dpg-ciolr45gkuvh5gh8pst0-a.oregon-postgres.render.com/tabuada_cluster_teste'

    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()

    filtered_rows = []
    for _, row in df.iterrows():
        class_value = row['class']
        question_value = row['question']

        query = f"SELECT * FROM tabuada WHERE class = {class_value} AND question = '{question_value}'"
        cur.execute(query)
        rows = cur.fetchall()
        filtered_rows.extend(rows)

    cur.close()
    conn.close()

    columns = [desc[0] for desc in cur.description]
    data = pd.DataFrame(filtered_rows, columns=columns)
    return data


# Clusterizar os dados
def clusterizar_dados(data):
    # Realizar a clusterização separadamente para cada pergunta
    for question in data["question"].unique():
        # Filtrar os dados para a pergunta atual
        filtered_data_question = data[data["question"] == question].copy()

        # Realizar a clusterização separadamente para cada classe dentro da pergunta
        for desired_class in filtered_data_question["class"].unique():
            # Filtrar os dados para a pergunta e classe atual
            filtered_data = filtered_data_question[filtered_data_question["class"] == desired_class].copy()

            # Verificar se há dados após o filtro
            if filtered_data.empty:
                continue

            # Selecionar as características relevantes
            features = filtered_data[["correct", "time"]]

            # Padronizar as variáveis
            scaler = StandardScaler()
            features_scaled = scaler.fit_transform(features)

            # Executar o algoritmo k-means com 2 clusters
            kmeans = KMeans(n_clusters=2)
            kmeans.fit(features_scaled)

            # Adicionar os rótulos dos clusters ao dataset filtrado
            filtered_data["cluster"] = kmeans.labels_

            # Verificar se o cluster contém os valores específicos na coluna 'id'
            low_performance_ids = filtered_data.loc[filtered_data["id_student"] == '1', "cluster"].unique()
            high_performance_ids = filtered_data.loc[filtered_data["id_student"] == '0', "cluster"].unique()

            # Inverter os rótulos aos clusters com base nos valores específicos na coluna 'id'
            filtered_data.loc[filtered_data["cluster"].isin(low_performance_ids), "cluster_label"] = "Baixo Desempenho"
            filtered_data.loc[filtered_data["cluster"].isin(high_performance_ids), "cluster_label"] = "Bom Desempenho"

            # Verificar se a coluna 'cluster_label' contém 'NaN' e alterar para 'Baixo Desempenho'
            filtered_data.loc[filtered_data["cluster_label"].isna(), "cluster_label"] = "Baixo Desempenho"

            # Calcular a distância da observação ao centro do cluster 'Bom Desempenho'
            bom_desempenho_center = kmeans.cluster_centers_[filtered_data.loc[filtered_data["cluster_label"] == "Bom Desempenho", "cluster"].values[0]]
            distances = cdist(features_scaled, [bom_desempenho_center], metric='euclidean')

            # Escalar as distâncias para a escala de 0 a 100
            scaler = MinMaxScaler(feature_range=(0, 100))
            proximity_scaled = scaler.fit_transform(distances)
            proximity_inverted = abs(proximity_scaled - scaler.feature_range[1])  # Invert the proximity values

            # Converter ndarray para uma lista Python antes de formatar
            proximity_inverted_list = proximity_inverted.tolist()

            # Formatar os valores da proximidade sem notação científica e com uma casa decimal
            proximity_formatted = ["{:.1f}".format(value[0]) for value in proximity_inverted_list]

            # Adicionar os resultados da clusterização ao dataset original
            data.loc[(data["question"] == question) & (data["class"] == desired_class), "cluster"] = filtered_data["cluster"]
            data.loc[(data["question"] == question) & (data["class"] == desired_class), "cluster_label"] = filtered_data["cluster_label"]
            data.loc[(data["question"] == question) & (data["class"] == desired_class), "proximity"] = proximity_formatted

    return data


# Transformar o Json em DataFrame
def json_to_dataframe(json_data):
    data = json_data
    hits = data["hits"]

    df_data = []
    for hit in hits:
        df_data.append([
            data.get("roomMult", ""),
            data.get("id_student", ""),
            data.get("class", ""),
            data.get("age", ""),
            hit["question"],
            hit["correct"],
            hit["time"]
        ])

    columns = ["roomMult", "id_student", "class", "age", "question", "correct", "time"]
    df = pd.DataFrame(df_data, columns=columns)
    return df

def insert_dataframe_to_postgresql(df):
    connection_string = 'postgres://tabuada_cluster_teste_user:hm2itcVVmeYdya6KoFeisFXbqTpUBua7@dpg-ciolr45gkuvh5gh8pst0-a.oregon-postgres.render.com/tabuada_cluster_teste'

    # Nome da tabela no PostgreSQL
    table_name = 'tabuada'

    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("INSERT INTO {} (id_student, question, class, time, correct) VALUES (%s, %s, %s, %s, %s)".format(table_name), (row['id_student'], row['question'], row['class'], row['time'], row['correct']))

    conn.commit()

    cur.close()
    conn.close()


# Atualizar Json
def add_cluster_data_to_hits(json_data, data_clustered):
    # Carregar o JSON em uma estrutura de dados
    data = json.loads(json_data)

    # Acessar a lista de hits
    hits = data['hits']

    # Acessar os valores de 'cluster_label' e 'proximity' no dataframe data_clustered
    cluster_labels = data_clustered['cluster_label'].tail(len(hits)).tolist()
    proximities = data_clustered['proximity'].tail(len(hits)).tolist()

    # Percorrer cada item na lista de hits
    for i, hit in enumerate(hits):
        # Adicionar os campos 'cluster_label' e 'proximity' aos hits
        hit['cluster_label'] = cluster_labels[i]
        hit['proximity'] = proximities[i]

    # Retornar o JSON atualizado
    return json.dumps(data, indent=4)










































