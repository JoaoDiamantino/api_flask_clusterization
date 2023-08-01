from flask import Flask, render_template, request, jsonify
import clusterization

app = Flask(__name__)


@app.route("/")
def home():
    return render_template("index.html")

@app.route('/predict', methods=['POST'])
def predict():
    json_data = request.form.get('content')
    df = clusterization.json_to_dataframe(json_data)
    clusterization.insert_dataframe_to_postgresql(df)
    data = clusterization.execute_query(df)
    clusterization.clusterizar_dados(data)
    prediction = clusterization.add_cluster_data_to_hits(json_data, data)
    return render_template("index.html", prediction=prediction, json_data=json_data)

# Create an API endpoint
@app.route('/api/predict', methods=['POST'])
def predict_api():
    json_data = request.get_json(force=True)  # Get data posted as a json
    df = clusterization.json_to_dataframe(json_data)
    clusterization.insert_dataframe_to_postgresql(df)
    data = clusterization.execute_query(df)
    clusterization.clusterizar_dados(data)
    prediction = clusterization.add_cluster_data_to_hits(json_data, data)
    return {'prediction': prediction, 'json_data': json_data}  # Return prediction

if __name__ == '__main__':
    app.run(debug=True)
