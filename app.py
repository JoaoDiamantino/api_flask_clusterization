from flask import Flask, request, render_template, jsonify

app = Flask(__name__)


@app.route('/upload', methods=['GET', 'POST'])
def upload_json():
    if request.method == 'POST':
        # Verifica se foi enviado um arquivo JSON
        if 'file' not in request.files:
            return jsonify({'error': 'Nenhum arquivo enviado.'}), 400

        file = request.files['file']

        # Verifica se o arquivo tem um nome
        if file.filename == '':
            return jsonify({'error': 'O arquivo não tem um nome.'}), 400

        # Verifica se o arquivo é um JSON
        if file.filename.endswith('.json'):
            try:
                # Faz a leitura do arquivo JSON
                data = file.read()

                # Aqui você pode fazer qualquer processamento necessário nos dados
                json_content = data.decode('utf-8')

                return render_template('result_json.html', json_content=json_content)

            except Exception as e:
                return jsonify({'error': str(e)}), 500

        return jsonify({'error': 'O arquivo enviado não é um JSON.'}), 400

    return render_template('upload.html')


@app.route('/input', methods = ['GET','POST'])
def input_values():
    if request.method == 'POST':
        class_value = request.form['class']
        question_value = request.form['question']
        correct_value = request.form['correct']
        time_value = request.form['time']

        return f"Valores inseridos: Class={class_value}, Question={question_value}, Correct={correct_value}, Time={time_value}"

    return render_template('input.html')

if __name__ == '__main__':
    app.run(debug=True)
